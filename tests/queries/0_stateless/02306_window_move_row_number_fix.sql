-- Tags: no-backward-compatibility-check
SELECT nth_value(NULL, 1048577) OVER (Rows BETWEEN 1023 FOLLOWING AND UNBOUNDED FOLLOWING)
