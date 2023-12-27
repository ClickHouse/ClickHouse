---
slug: /en/sql-reference/streaming/streaming_query
---

Enable streaming query over Kafka table engine by specifying `EMIT STREAM` clause.
Streaming query never ends until gets cancelled. It monitors the changes (new data) in 
Kafka topic continuously and emit the (intermediate) results continuously. 

**Syntax**

```sql
EMIT STREAM [PERIODIC interval] 
```

The **interval** could be regular ClickHouse interval expression like `INTERVAL 1 SECOND` or shortcuts like `1s`.

Examples:

Streaming tail Kafka table engine.

```sql
SELECT * FROM kafka_queue EMIT STREAM;
```

Streaming aggregation over Kafka table engine.

```sql
SELECT count() FROM kafka_queue EMIT STREAM PERIODIC 1s; -- Emit intermediate aggregation results every 1 second
```
