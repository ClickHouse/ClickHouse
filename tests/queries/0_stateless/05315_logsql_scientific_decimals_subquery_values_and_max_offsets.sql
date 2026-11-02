-- Regressions for the exact comparison of decimal literals written in scientific notation,
-- for the normalization of the values returned by the subquery form of `contains_any` and
-- `contains_all`, for `contains_all` over an empty subquery result, and for the maximum
-- `UInt64` offsets in the per-partition `sort` and in `running_stats`/`total_stats`.

DROP TABLE IF EXISTS logs_05315;
CREATE TABLE logs_05315
(
    `_time` DateTime,
    `_msg` String,
    `dec` Decimal128(25),
    `code` UInt64,
    `note` Nullable(String),
    `p` String,
    `v` UInt64
) ENGINE = MergeTree ORDER BY _time;

INSERT INTO logs_05315 VALUES
    ('2024-01-01 00:00:00', 'status 500 error', 10.5, 500, NULL, 'a', 1),
    ('2024-01-01 00:00:01', 'status 404 missing', 10.50000000000000000001, 404, NULL, 'a', 2),
    ('2024-01-01 00:00:02', 'status 200 ok', 10.50000000000000000002, 0, NULL, 'b', 3),
    ('2024-01-01 00:00:03', 'status 5000 other', 11, 0, NULL, 'b', 4);

SET enable_logsql_dialect = 1;
SET logsql_table = 'logs_05315';
SET dialect = 'logsql';

-- A decimal literal in scientific notation is compared with its exact value,
-- like its plain spelling, instead of the value rounded through `Float64`.
dec:=10.50000000000000000001 | fields dec;
dec:=10.50000000000000000001e0 | fields dec;
dec:=1050000000000000000001E-20 | fields dec;
dec:=0.1050000000000000000001e+2 | fields dec;
dec:range[10.50000000000000000001e0, 10.50000000000000000002e0] | fields dec | sort by (dec);
dec:>10.5e0 | fields dec | sort by (dec);
dec:<10.50000000000000000001e0 | fields dec;

-- The subquery values are normalized to LogsQL strings: a numeric subquery output
-- matches as text, with word boundaries, like the literal form.
_msg:contains_any(code:>0 | fields code) | fields _msg | sort by (_msg);
_msg:contains_any(500, 404) | fields _msg | sort by (_msg);
_msg:contains_all(code:500 | fields code) | fields _msg;
-- A missing `Nullable` value is the empty LogsQL value, which matches everything.
_msg:contains_any(v:1 | fields note) | count();

-- `contains_all` over an empty subquery result matches nothing, like `contains_all()`.
_msg:contains_all(code:999 | fields code) | count();
_msg:contains_all() | count();
_msg:contains_any(code:999 | fields code) | count();

-- `offset + limit` beyond the `UInt64` range keeps every rank after the offset.
* | sort by (v) partition by (p) offset 1 limit 18446744073709551615 | fields p, v | sort by (v);
* | sort by (v) partition by (p) offset 18446744073709551615 limit 18446744073709551615 | count();

-- An offset beyond any frame means "past the end", up to the maximum `UInt64` value.
* | running_stats first(v) offset 18446744073709551615 as f | fields _time, f | sort by (_time);
* | running_stats first(v) offset 9223372036854775807 as f | fields _time, f | sort by (_time);
* | running_stats last(v) offset 18446744073709551615 as l | fields _time, l | sort by (_time);
* | total_stats first(v) offset 18446744073709551615 as f | fields _time, f | sort by (_time);
* | total_stats last(v) offset 18446744073709551615 as l | fields _time, l | sort by (_time);
* | total_stats first(v) offset 1 as f | fields _time, f | sort by (_time);

SET dialect = 'clickhouse';
DROP TABLE logs_05315;
