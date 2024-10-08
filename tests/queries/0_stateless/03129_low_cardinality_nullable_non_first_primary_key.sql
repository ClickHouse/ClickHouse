DROP TABLE IF EXISTS small;

CREATE TABLE small (`dt` DateTime, `user_email` LowCardinality(Nullable(String)))
ENGINE = MergeTree order by (dt, user_email) settings allow_nullable_key = 1, min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO small (dt, user_email) SELECT number, if(number % 3 = 2, NULL, number) FROM numbers(1e2);

SELECT SUM(dt::int) FROM small WHERE user_email IS NULL;

DROP TABLE small;
