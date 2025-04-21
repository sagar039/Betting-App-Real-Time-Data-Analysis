# Betting-App-Real-Time-Data-Analysis
# 🎲 Betting App Real-Time Data Analysis (Industrial Project)

A real-time data processing pipeline built for analyzing betting application events using AWS streaming services and Apache Flink. This project simulates high-velocity streaming data, processes it with PyFlink, and creates a queryable data lake using AWS S3, Glue Catalog, and Athena.

---

## 🚀 Tech Stack

- **Programming Language**: Python
- **Stream Processing**: Apache Flink (PyFlink), AWS Managed Flink
- **Data Streaming**: AWS Kinesis (Data Streams)
- **Data Lake Storage**: AWS S3
- **Data Ingestion**: AWS Data Firehose
- **Metadata Management**: AWS Glue Catalog
- **Query Engine**: AWS Athena
- **Build Tool**: Maven

---

## 📌 Project Objectives

- Simulate and process real-time betting transactions
- Perform stream transformations and window-based joins
- Persist processed data in a data lake
- Query structured streaming data for analytical insights

---

## 🛠️ Key Implementation Steps

### 1. **Local Environment Setup**
- Installed and configured **Apache Flink** and **Maven**.
- Set up **Kinesis Input and Output streams** using AWS Console or CLI.

### 2. **PyFlink Application Development**
- Created a **PyFlink application** using the **DataStream API** to:
  - Read real-time data from **Kinesis input stream**.
  - Validate and clean incoming betting events.
  - Perform **windowed joins** and transformation logic.
  - Write transformed data to **Kinesis output stream**.

### 3. **Build & Test Locally**
- Packaged the Flink application using Maven.
- Published mock data to the **Kinesis stream** using a Python producer script.
- Verified functionality by running the Flink job **locally**.

### 4. **Deploy to AWS Managed Flink**
- Uploaded the application JAR to S3.
- Deployed the job on **AWS Managed Flink** using runtime parameters and monitored via AWS Console.

### 5. **Data Ingestion to S3 via Firehose**
- Created an **AWS Data Firehose** stream to pull data from Kinesis and write to **S3** in mini-batches.

### 6. **Data Lake and Analytics**
- Set up **AWS Glue Crawler** to scan the S3 data and create a schema in **AWS Glue Catalog**.
- Ran **SQL-based aggregation and analysis** queries using **Athena** directly on the processed data.

---

## 📈 Sample Use Case

The system can answer questions like:
- Which users are placing the most bets in the last 15 minutes?
- What is the average bet amount by game type in real-time?
- Identify patterns that could indicate suspicious or high-risk betting behavior.

---

---

## ✅ Future Enhancements

- Integrate alerts/notifications for fraudulent betting patterns.
- Use AWS Lambda or SNS for event-driven post-processing.
- Implement schema validation using Apache Avro or Protobuf.

---

## 📬 Contact

If you have any questions or want to collaborate, feel free to reach out via [GitHub Issues](https://github.com/sagar039/Betting-App-Real-Time-Data-Analysis/issues).

---

## 🧠 Learnings

This project provided hands-on experience in:
- Real-time stream processing with **Apache Flink & PyFlink**
- Working with AWS **Kinesis, Firehose, S3, Glue, Athena**
- End-to-end data engineering pipeline on **cloud infrastructure**

---
