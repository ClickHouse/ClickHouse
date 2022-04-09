---
sidebar_label: Kafka Connect HTTP Connector
sidebar_position: 7
description: Using HTTP Connector Sink with Kafka Connect and ClickHouse
---

# HTTP Sink Connector

The HTTP Sink Connector has several advantages over the JDBC approach. Principally, it is data type agnostic and thus does not need a Kafka schema as well as supporting ClickHouse specific data types such as Maps and Arrays. This additional flexibility comes at a slight increase in configuration complexity.

We repeat the example of pulling messages from a single Kafka topic and inserting rows into a ClickHouse table. Note that this example preserves the Array fields of the Github dataset (which are removed for the JDBC Sink example via the insertion script). We assume you have an empty github topic in the examples and use kcat for message insertion to Kafka.

## Self-Managed

### Steps

#### 1. Install Kafka Connect and Connector

Download the Confluent package and install it locally. Follow the installation instructions for installing the connector as documented [here](https://docs.confluent.io/kafka-connect-http/current/overview.html).

If you use the confluent-hub installation method, your local configuration files will be updated.

#### 2. Prepare Configuration

Follow [these instructions](https://docs.confluent.io/cloud/current/cp-component/connect-cloud-config.html#set-up-a-local-connect-worker-with-cp-install) for setting up Connect relevant to your installation type, noting the differences between a standalone and distributed cluster. If using Confluent Cloud, the distributed setup is relevant.

The most important parameter is the `http.api.url`. The [HTTP interface](https://clickhouse.com/docs/en/interfaces/http/) for ClickHouse requires you to encode the INSERT statement as a parameter in the URL. This must include the format (`JSONEachRow` in this case) and target database. The format must be consistent with the Kafka data, which will be converted to a string in the HTTP payload. These parameters must be URL escaped. An example of this format for the Github dataset (assuming you are running ClickHouse locally) is shown below:

```
<protocol>://<clickhouse_host>:<clickhouse_port>?query=INSERT%20INTO%20<database>.<table>%20FORMAT%20JSONEachRow

http://localhost:8123?query=INSERT%20INTO%20default.github%20FORMAT%20JSONEachRow
```

**This URL is error-prone. Ensure escaping is precise to avoid issues.**

The following additional parameters are relevant to using the HTTP Sink with ClickHouse. A complete parameter list can be found [here](https://docs.confluent.io/kafka-connect-http/current/connector_config.html):


* `request.method` - Set to POST**
* `retry.on.status.codes` - Set to 400-500 to retry on any error codes. Refine based expected errors in data.
* `request.body.format` - In most cases this will be JSON.
* `auth.type` - Set to BASIC if you security with ClickHouse. Other ClickHouse compatible authentication mechanisms are not currently supported.
* `ssl.enabled` - set to true if using SSL.
* `headers` - set to "Content-Type: application/json"
* `connection.user` - username for ClickHouse.
* `connection.password` - password for ClickHouse.
* `batch.max.size` - The number of rows to send in a single batch. Ensure this set is to an appropriately large number. Per ClickHouse [recommendations](https://clickhouse.com/docs/en/introduction/performance/#performance-when-inserting-data) a value of 1000 is should be considered a minimum.
* `tasks.max` - The HTTP Sink connector supports running one or more tasks. This can be used to increase performance. Along with batch size this represents your primary means of improving performance.
* `key.converter` - set according to the types of your keys.
* `value.converter` - set based on the type of data on your topic. This data does not need a schema. The format here must be consistent with the FORMAT specified in the parameter `http.api.url`. The simplest here is to use JSON and the org.apache.kafka.connect.json.JsonConverter converter. Treating the value as a string, via the converter org.apache.kafka.connect.storage.StringConverter, is also possible - although this will require the user to extract a value in the insert statement using functions. Avro format is also supported in [ClickHouse](https://clickhouse.com/docs/en/interfaces/formats/#data-format-avro) if using the io.confluent.connect.avro.AvroConverter converter.  

A full list of settings, including how to configure a proxy, retries, and advanced SSL, can be found [here](https://docs.confluent.io/kafka-connect-http/current/connector_config.html).

Example configuration files for the Github sample data can be found [here](https://github.com/ClickHouse/clickhouse-docs/tree/main/docs/integrations/kafka/code/connectors/http_sink), assuming Connect is run in standalone mode and Kafka is hosted in Confluent Cloud.

#### 3. Create the ClickHouse table

Ensure the table has been created. An example for a minimal github dataset using a standard MergeTree is shown below.


```sql
CREATE TABLE github
(
    file_time DateTime,
    event_type Enum('CommitCommentEvent' = 1, 'CreateEvent' = 2, 'DeleteEvent' = 3, 'ForkEvent' = 4,'GollumEvent' = 5, 'IssueCommentEvent' = 6, 'IssuesEvent' = 7, 'MemberEvent' = 8, 'PublicEvent' = 9, 'PullRequestEvent' = 10, 'PullRequestReviewCommentEvent' = 11, 'PushEvent' = 12, 'ReleaseEvent' = 13, 'SponsorshipEvent' = 14, 'WatchEvent' = 15, 'GistEvent' = 16, 'FollowEvent' = 17, 'DownloadEvent' = 18, 'PullRequestReviewEvent' = 19, 'ForkApplyEvent' = 20, 'Event' = 21, 'TeamAddEvent' = 22),
    actor_login LowCardinality(String),
    repo_name LowCardinality(String),
    created_at DateTime,
    updated_at DateTime,
    action Enum('none' = 0, 'created' = 1, 'added' = 2, 'edited' = 3, 'deleted' = 4, 'opened' = 5, 'closed' = 6, 'reopened' = 7, 'assigned' = 8, 'unassigned' = 9, 'labeled' = 10, 'unlabeled' = 11, 'review_requested' = 12, 'review_request_removed' = 13, 'synchronize' = 14, 'started' = 15, 'published' = 16, 'update' = 17, 'create' = 18, 'fork' = 19, 'merged' = 20),
    comment_id UInt64,
    path String,
    ref LowCardinality(String),
    ref_type Enum('none' = 0, 'branch' = 1, 'tag' = 2, 'repository' = 3, 'unknown' = 4),
    creator_user_login LowCardinality(String),
    number UInt32,
    title String,
    labels Array(LowCardinality(String)),
    state Enum('none' = 0, 'open' = 1, 'closed' = 2),
    assignee LowCardinality(String),
    assignees Array(LowCardinality(String)),
    closed_at DateTime,
    merged_at DateTime,
    merge_commit_sha String,
    requested_reviewers Array(LowCardinality(String)),
    merged_by LowCardinality(String),
    review_comments UInt32,
    member_login LowCardinality(String)
) ENGINE = MergeTree ORDER BY (event_type, repo_name, created_at)

```

#### 4. Add data to Kafka

Insert messages to Kafka. Below we use kcat to insert 10k messages.

```bash
head -n 10000 github_all_columns.ndjson | kafkacat -b <host>:<port> -X security.protocol=sasl_ssl -X sasl.mechanisms=PLAIN -X sasl.username=<username>  -X sasl.password=<password> -t github
```

A simple read on the target table “Github” should confirm the insertion of data.


```sql
SELECT count() FROM default.github;

| count\(\) |
| :--- |
| 10000 |

```

## Confluent Cloud

A fully managed version of HTTP Sink is available for those using Confluent Cloud for their Kafka hosting. This requires your ClickHouse environment to be accessible from Confluent Cloud. We assume you have taken the appropriate measures to secure this.

The instructions for creating an HTTP Sink in Confluent Cloud can be found [here](https://docs.confluent.io/cloud/current/connectors/cc-http-sink.html). The following settings are relevant if connecting to ClickHouse. If not specified, form defaults are applicable:


* `Input messages` - Depends on your source data but in most cases JSON or Avro. We assume JSON in the following settings.
* `Kafka Cluster credentials` - Confluent cloud allows you to generate these for the appropriate topic from which you wish to pull messages.
* HTTP server details - The connection details for ClickHouse. Specifically:
    * `HTTP Url` - This should be of the same format as the self-managed configuration parameter `http.api.url i.e. &lt;protocol>://&lt;clickhouse_host>:&lt;clickhouse_port>?query=INSERT%20INTO%20&lt;database>.&lt;table>%20FORMAT%20JSONEachRow`
    * `HTTP Request Method` - Set to POST
    * `HTTP Headers` - “Content Type: application/json”
* HTTP server batches
    * `Request Body Format` - json
    * `Batch batch size` - Per ClickHouse recommendations, set this to at least 1000.
* HTTP server authentication
    * `Endpoint Authentication type` - BASIC
    * `Auth username` - ClickHouse username
    * `Auth password` - ClickHouse password
* HTTP server retries - Settings here can be adjusted according to requirements. Timeouts specifically may need adjusting depending on latency.
    * `Retry on HTTP codes` - 400-500 but adapt as required e.g. this may change if you have an HTTP proxy in front of ClickHouse.
    * `Maximum Reties` - the default (10) is appropriate but feel to adjust for more robust retries.

<img src={require('./images/kafka_05.png').default} class="image" alt="Connecting Confluent HTTP Sink" style={{width: '50%'}}/>
