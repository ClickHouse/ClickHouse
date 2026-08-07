CREATE TEMPORARY TABLE decimal
(
    f dec(38, 38)
);

INSERT INTO decimal VALUES (0);
INSERT INTO decimal VALUES (0.42);
INSERT INTO decimal VALUES (-0.42);

SELECT f + 1048575, f - 21, f - 84, f * 21, f * -21, f / 21, f / 84 FROM decimal WHERE f > 0; -- { serverError DECIMAL_OVERFLOW }
SELECT f + -2, f - 21, f - 84, f * 21, f * -21, f / 9223372036854775807, f / 84 FROM decimal WHERE f > 0; -- { serverError DECIMAL_OVERFLOW }
