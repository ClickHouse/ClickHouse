-- The type of an EPHEMERAL column is kept as a string, as the argument of `defaultValueOfTypeName`.
-- That string used to be produced by `formatForLogging`, which runs the server's
-- `query_masking_rules` over it. The rules in `tests/config/config.d/query_masking_rules.xml`
-- replace `TOPSECRET.TOPSECRET` with `[hidden]`, which turned the type below into an `Enum8` with
-- two elements of the same name, and `CREATE TABLE` failed with `Duplicate names in enum`.

DROP TABLE IF EXISTS t_ephemeral_masked_type;

CREATE TABLE t_ephemeral_masked_type
(
    id UInt8,
    e Enum8('TOPSECRET.TOPSECRET' = 1, '[hidden]' = 2) EPHEMERAL,
    x String DEFAULT toString(e)
)
ENGINE = Memory;

INSERT INTO t_ephemeral_masked_type (id) VALUES (1);

SELECT x FROM t_ephemeral_masked_type;

DROP TABLE t_ephemeral_masked_type;
