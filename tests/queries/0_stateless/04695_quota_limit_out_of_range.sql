-- A quota limit of a type with a denominator (`execution_time` is kept in nanoseconds) is scaled with floating
-- point arithmetic. A value whose scaled form does not fit into `UInt64` used to be converted with a plain
-- `static_cast`, which is undefined behavior, reported by UBSan as
-- "1.84467e+19 is outside the range of representable values of type 'unsigned long'".

DROP QUOTA IF EXISTS quota_04695;
CREATE QUOTA quota_04695 FOR INTERVAL 1 day MAX execution_time = 1e19; -- { error BAD_ARGUMENTS }
CREATE QUOTA quota_04695 FOR INTERVAL 1 day MAX execution_time = 18446744073709551616; -- { error BAD_ARGUMENTS }
CREATE QUOTA quota_04695 FOR INTERVAL 1 day MAX execution_time = inf; -- { error BAD_ARGUMENTS }
CREATE QUOTA quota_04695 FOR INTERVAL 1 day MAX execution_time = -1e19; -- { error BAD_ARGUMENTS }
CREATE QUOTA quota_04695 FOR INTERVAL 1 day MAX execution_time = 1.5;
SELECT max_execution_time FROM system.quota_limits WHERE quota_name = 'quota_04695';
DROP QUOTA quota_04695;
