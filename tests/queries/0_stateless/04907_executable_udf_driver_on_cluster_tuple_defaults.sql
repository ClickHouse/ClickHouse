-- Executable-UDF driver signatures must be validated on the initiator before an `ON CLUSTER`
-- query is sent to workers. Tuple-element `DEFAULT` expressions are valid only in column declarations.
CREATE FUNCTION executable_udf_driver_default_argument
    ON CLUSTER test_shard_localhost
    ARGUMENTS (x Tuple(a UInt8 DEFAULT 1))
    RETURNS UInt8
    ENGINE = nonexistent_driver()
    AS 'ignored'; -- { serverError BAD_ARGUMENTS }

CREATE FUNCTION executable_udf_driver_default_return
    ON CLUSTER test_shard_localhost
    ARGUMENTS (x UInt8)
    RETURNS Tuple(b UInt8 DEFAULT 2)
    ENGINE = nonexistent_driver()
    AS 'ignored'; -- { serverError BAD_ARGUMENTS }
