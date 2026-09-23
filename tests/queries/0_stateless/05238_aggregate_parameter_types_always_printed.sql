-- A parameter value with no unambiguous bare literal form is printed with a ::Type suffix, so the
-- state type name reparses into the same parameters. A value that needs no suffix keeps none.
-- The suffix goes on the elements of an array or tuple parameter, so the name of such a parameter
-- is a function call over literals rather than one literal.

DROP TABLE IF EXISTS t_ap_05238;

CREATE TABLE t_ap_05238
(
    control_bare  AggregateFunction(groupArrayInsertAt(7, 3), UInt64, UInt32),
    typed_decimal AggregateFunction(groupArrayInsertAt('1.5'::Decimal32(1), 3), Decimal32(1), UInt32),
    typed_uuid    AggregateFunction(groupArrayInsertAt('11111111-1111-1111-1111-111111111111'::UUID, 3), UUID, UInt32),
    typed_array   AggregateFunction(1, sumMapFiltered(['1'::Decimal32(1)]), Array(Decimal32(1)), Array(UInt64))
)
ENGINE = MergeTree ORDER BY tuple();

SELECT type FROM system.columns
WHERE database = currentDatabase() AND table = 't_ap_05238' ORDER BY name;

-- A state built by a query must print the same name as the declared column.
SELECT toTypeName(groupArrayInsertAtState(toDecimal32(1.5, 1), 3)(x, i))
FROM (SELECT toDecimal32(2.5, 1) AS x, toUInt32(0) AS i);

SELECT toTypeName(sumMapFilteredState([toDecimal32(1, 1)])(k, v))
FROM (SELECT [toDecimal32(1, 1)] AS k, [toUInt64(10)] AS v);

INSERT INTO t_ap_05238 VALUES (
    initializeAggregation('groupArrayInsertAtState(7, 3)', 5::UInt64, 0::UInt32),
    initializeAggregation('groupArrayInsertAtState(\'1.5\'::Decimal32(1), 3)', toDecimal32(2.5, 1), 0::UInt32),
    initializeAggregation('groupArrayInsertAtState(\'11111111-1111-1111-1111-111111111111\'::UUID, 3)',
                          toUUID('22222222-2222-2222-2222-222222222222'), 0::UInt32),
    initializeAggregation('sumMapFilteredState([\'1\'::Decimal32(1)])',
                          [toDecimal32(1, 1)], [toUInt64(10)]));

-- Re-reads and reparses the metadata, which is what the server does for every table on startup.
DETACH TABLE t_ap_05238;
ATTACH TABLE t_ap_05238;

SELECT finalizeAggregation(control_bare), finalizeAggregation(typed_decimal),
       finalizeAggregation(typed_uuid), finalizeAggregation(typed_array) FROM t_ap_05238;

DROP TABLE t_ap_05238;
