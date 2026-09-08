-- An exact numeric constant compared to `Date32` must respect the representable window
-- `[0000-01-01, 9999-12-31]` = day numbers `[-719528, 2932896]`: the boundaries match,
-- while a constant one step outside converts to Null in `convertFieldToType` and matches
-- nothing, instead of passing through as an impossible raw day number.
SELECT toDate32('0000-01-01') IN (-719528);
SELECT toDate32('0000-01-01') IN (-719529);
SELECT toDate32('9999-12-31') IN (2932896);
SELECT toDate32('9999-12-31') IN (2932897);
-- An out-of-range constant in the set does not disturb matching of the valid ones.
SELECT toDate32('1970-01-02') IN (1, -719529, 2932897);

-- The same exact-bound conversion drives index analysis over a `Date32` primary key.
DROP TABLE IF EXISTS t_date32_numeric_field_range;
CREATE TABLE t_date32_numeric_field_range (d Date32) ENGINE = MergeTree ORDER BY d;
INSERT INTO t_date32_numeric_field_range VALUES ('0000-01-01') ('1970-01-02') ('9999-12-31');
SELECT d FROM t_date32_numeric_field_range WHERE d IN (-719528, 2932896) ORDER BY d;
SELECT d FROM t_date32_numeric_field_range WHERE d IN (-719529, 1, 2932897) ORDER BY d;
DROP TABLE t_date32_numeric_field_range;
