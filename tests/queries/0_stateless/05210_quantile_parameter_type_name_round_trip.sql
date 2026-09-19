-- A quantile level may be a Decimal, a wide integer or a UUID. The printed state type name is what
-- lands in the table metadata, and the server parses every table's metadata on startup, so a name
-- that does not parse back into the same parameters makes the server refuse to start.

DROP TABLE IF EXISTS t_qp_05210;

CREATE TABLE t_qp_05210
(
    d AggregateFunction(quantile(0.5::Decimal32(1)), Float64),
    w AggregateFunction(quantile(1::Int256), Float64),
    u AggregateFunction(quantile('00000000-0000-0000-0000-000000000000'::UUID), Float64),
    t AggregateFunction(quantileExactTuple(0.5::Decimal32(1)), Tuple(Float64))
)
ENGINE = MergeTree ORDER BY tuple();

SELECT type FROM system.columns
WHERE database = currentDatabase() AND table = 't_qp_05210'
ORDER BY name;

INSERT INTO t_qp_05210 VALUES (
    initializeAggregation('quantileState(0.5::Decimal32(1))', 1.0),
    initializeAggregation('quantileState(1::Int256)', 2.0),
    initializeAggregation('quantileState(\'00000000-0000-0000-0000-000000000000\'::UUID)', 3.0),
    initializeAggregation('quantileExactTupleState(0.5::Decimal32(1))', tuple(4.0)));

-- Re-reads and re-parses the metadata, which is what the server does for every table on startup.
DETACH TABLE t_qp_05210;
ATTACH TABLE t_qp_05210;

SELECT finalizeAggregation(d), finalizeAggregation(w), finalizeAggregation(u), finalizeAggregation(t)
FROM t_qp_05210;

DROP TABLE t_qp_05210;

-- Folding a constant argument reaches the same printer with no type-spec syntax in the query.
SELECT toTypeName(quantileState(toDecimal32(0.5, 1))(number)) FROM numbers(1);
