-- Validate that map_buckets_coefficient must be positive and finite.

CREATE TABLE t_map_coeff_neg (m Map(String, UInt64)) ENGINE = MergeTree ORDER BY tuple() SETTINGS map_buckets_coefficient = -1.0; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_map_coeff_zero (m Map(String, UInt64)) ENGINE = MergeTree ORDER BY tuple() SETTINGS map_buckets_coefficient = 0.0; -- { serverError BAD_ARGUMENTS }
-- nan and inf are rejected earlier by the Float setting parser.
CREATE TABLE t_map_coeff_nan (m Map(String, UInt64)) ENGINE = MergeTree ORDER BY tuple() SETTINGS map_buckets_coefficient = nan; -- { serverError CANNOT_PARSE_NUMBER }
CREATE TABLE t_map_coeff_inf (m Map(String, UInt64)) ENGINE = MergeTree ORDER BY tuple() SETTINGS map_buckets_coefficient = inf; -- { serverError CANNOT_PARSE_NUMBER }

-- Positive finite values should be accepted.
DROP TABLE IF EXISTS t_map_coeff_ok;
CREATE TABLE t_map_coeff_ok (m Map(String, UInt64)) ENGINE = MergeTree ORDER BY tuple() SETTINGS map_buckets_coefficient = 0.5;
DROP TABLE t_map_coeff_ok;
