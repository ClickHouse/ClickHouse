CREATE TABLE 03094_grouparrysorted_dest
(
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    -- aggregates
    SlowSpans AggregateFunction(groupArraySorted(100),
        Tuple(NegativeDurationNs Int64, Timestamp DateTime64(9), TraceId String, SpanId String)
    ) CODEC(ZSTD(1))
)
ENGINE = AggregatingMergeTree()
ORDER BY (ServiceName);

CREATE TABLE 03094_grouparrysorted_src
(
    ServiceName String,
    Duration Int64,
    Timestamp DateTime64(9),
    TraceId String,
    SpanId String
)
ENGINE = MergeTree()
ORDER BY ();

CREATE MATERIALIZED VIEW 03094_grouparrysorted_mv TO 03094_grouparrysorted_dest
AS SELECT
   ServiceName,
   groupArraySortedState(100)(
                         CAST(
                             tuple(-Duration, Timestamp, TraceId, SpanId),
                             'Tuple(NegativeDurationNs Int64, Timestamp DateTime64(9), TraceId String, SpanId String)'
                         )) as SlowSpans
FROM 03094_grouparrysorted_src
GROUP BY
    ServiceName;


INSERT INTO 03094_grouparrysorted_src SELECT * FROM generateRandom() LIMIT 500000;
