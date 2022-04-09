# Kafka Producer

Supports [ClickHouse documentation on Kafka](../../).

This is a simple Kafka producer written in Python for ndjson data. It ensures a schema is set for the JSON - either generating a schema or using a specified one.

Schemas are required for tools such as the Kafka JDBC sink.

## Requirements

- Python 3.8.10+
- Kafka instance v7+. Easiest solution is to create a Kafka cluster in Confluent Cloud - which offers an adequate free tier.
- Ndjson file. A sample github ndjson file can be found [here](https://datasets-documentation.s3.eu-west-3.amazonaws.com/kafka/github_all_columns.ndjson) with accompanying config for the script [here](https://github.com/ClickHouse/kafka-samples/blob/main/producer/github.config). See [Larger Datasets](#larger-datasets) if a larger test file is required.

## Setup

`pip install -r requirements.txt`

## Usage

1. Prepare a configuration. See [github.config](https://github.com/ClickHouse/kafka-samples/blob/main/producer/github.config) for examples. Any target topic will be automatically created if it doesn't exist.
2. (Optional) Prepare a [JSON schema file](https://json-schema.org/) for your ndjson and specify this in the config from (1) via `input.schema`. To infer a schema automatically do not set this parameter. This will cause the schema to be inferred from the first 100 lines. This is best effort only (but works for the gitub dataset)!
3. Run it!

`python producer.py -c <config_file>`

## Not in scope

Whilst all producer configuration parameters supported by the [Kafka python client](https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html) can be used - replace `_` with `.` in the configuration, no work has been done regards testing these settings for optimal performance.

## Large Datasets

The sample [Github dataset](https://datasets-documentation.s3.eu-west-3.amazonaws.com/kafka/github_all_columns.ndjson) consists of events on the ClickHouse Github repository. This static files covers the period `2019-09-23` to `2022-01-05`.

Specifically, this file was generated from the following command executed against the [ClickHouse play site](https://ghe.clickhouse.tech/#clickhouse-demo-access):

```bash
clickhouse-client --secure --host play.clickhouse.com --port 9440 --user explorer --query "SELECT file_time, event_type, actor_login, repo_name, created_at, updated_at, action, comment_id, path, ref, ref_type, creator_user_login, number, title, labels, state, assignee, assignees, closed_at, merged_at, merge_commit_sha, requested_reviewers, merged_by, review_comments, member_login FROM github_events WHERE repo_name = 'ClickHouse/ClickHouse' ORDER BY created_at ASC LIMIT 200000 FORMAT JSONEachRow" > github_all_columns.ndjson
```

Note the upper limit 200k rows and restriction to the `ClickHouse/ClickHouse` repository. Feel free to use this command to generate larger datasets for testing, potentially exploring other repositories. If you experience quota limits, instructions for downloading and transforming the data can be found [here](https://ghe.clickhouse.tech/#download-the-dataset).