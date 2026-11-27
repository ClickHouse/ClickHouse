-- Test PREWHERE on a Buffer table with no destination: supportsPrewhere() must
-- fall through the null-destination guard and reject with ILLEGAL_PREWHERE, not crash.
DROP TABLE IF EXISTS buf_no_dest;
CREATE TABLE buf_no_dest (x UInt64, y UInt64) ENGINE = Buffer('', '', 1, 100, 200, 1000000, 10000000, 100000000, 1000000000);
INSERT INTO buf_no_dest VALUES (1, 2) (3, 4) (5, 6);

SELECT count() FROM buf_no_dest PREWHERE x > 2; -- { serverError ILLEGAL_PREWHERE }

SELECT x, y FROM buf_no_dest WHERE x > 2 ORDER BY x;
SELECT count() FROM buf_no_dest WHERE x > 2 SETTINGS optimize_move_to_prewhere = 1;

DROP TABLE buf_no_dest;
