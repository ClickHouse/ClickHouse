---
sidebar_label: Introduction 
sidebar_position: 1
description: Introduction to Kafka with ClickHouse
---


# Connecting Kafka

[Apache Kafka](https://kafka.apache.org/) is an open-source distributed event streaming platform used by thousands of companies for high-performance data pipelines, streaming analytics, data integration, and mission-critical applications. In most cases, users will wish to insert Kafka based data into ClickHouse - although the reverse is supported. Below we outline several options for both use cases, identifying the pros and cons of each approach. 

For those who do not have a Kafka instance to hand, we recommend [Confluent Cloud](https://www.confluent.io/get-started/), which offers a free tier adequate for testing these examples. For self-managed alternatives, consider the [Confluent for Kubernetes](https://docs.confluent.io/operator/current/overview.html) or [here](https://docs.confluent.io/platform/current/installation/installing_cp/overview.html) for non-Kubernetes environments. 


## Assumptions

* You are familiar with the Kafka fundamentals, such as producers, consumers and topics.
* You have a topic prepared for these examples. We assume all data is stored in Kafka as JSON, although the principles remain the same if using Avro.
* We utilise the excellent [kcat](https://github.com/edenhill/kcat) (formerly kafkacat) in our examples to publish and consume Kafka data.
* Whilst we reference some python scripts for loading sample data, feel free to adapt the examples to your dataset.
* You are broadly familiar with ClickHouse materialized views.