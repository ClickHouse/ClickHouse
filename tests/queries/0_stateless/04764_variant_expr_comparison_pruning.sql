-- A comparison constant converted to a `Variant` expression's type is not an exact point:
-- `'ab'::FixedString(4)` and `'ab'::FixedString(2)` convert to different values yet match the same row,
-- and `equals` may compare within each row's own alternative. Every count must be 1.

DROP TABLE IF EXISTS t_fs;
CREATE TABLE t_fs (v Variant(FixedString(4), UInt64), t Tuple(Variant(FixedString(4), UInt64))) ENGINE = Memory;
INSERT INTO t_fs VALUES ('ab'::FixedString(4), tuple('ab'::FixedString(4)));

-- The `AND` chain must be neither pruned nor folded to `false`, also with the `Variant` nested and with
-- `optimize_redundant_comparisons` disabled.
SET use_variant_default_implementation_for_comparisons = 0;
SELECT count() FROM t_fs WHERE v = 'ab'::FixedString(4) AND v = 'ab'::FixedString(2);
SELECT count() FROM t_fs WHERE t = tuple('ab'::FixedString(4)) AND t = tuple('ab'::FixedString(2));
SELECT count() FROM t_fs WHERE v = 'ab'::FixedString(4) AND v = 'ab'::FixedString(2) SETTINGS optimize_redundant_comparisons = 0;

-- The `OR` chain must not become `IN`, whose set holds `'7'` only as a `String`. The top-level case is in 04628.
DROP TABLE IF EXISTS t_str;
CREATE TABLE t_str (t Tuple(Variant(String, UInt64))) ENGINE = Memory;
INSERT INTO t_str VALUES (tuple(7::UInt64));

SET use_variant_default_implementation_for_comparisons = 1;
SELECT count() FROM t_str WHERE t = tuple('7') OR t = tuple('8') OR t = tuple('9');

DROP TABLE t_fs;
DROP TABLE t_str;
