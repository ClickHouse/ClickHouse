SET session_timezone = 'UTC';

DROP TABLE IF EXISTS logs_05042;
CREATE TABLE logs_05042
(
    `_time` DateTime,
    `_msg` String,
    `size` UInt64,
    `price` Decimal64(2),
    `note` Nullable(String),
    `val` String
) ENGINE = MergeTree ORDER BY _time;

INSERT INTO logs_05042 VALUES
    ('2024-01-01 00:00:00', '2024-01-02T03:04:05+01:23', 1234, 10.50, NULL, '10'),
    ('2024-01-01 00:00:01', '2024-01-02T03:04:05+123:456', 56, 2.25, 'hello', '2.5'),
    ('2024-01-01 00:00:02', '2024-01-02 03:04:05Z', 7, 3.00, '', 'inf'),
    ('2024-01-01 00:00:03', '2024-01-02T03:04:05-05:00', 8, 4.75, 'w', 'information');

SET allow_experimental_logsql_dialect = 1;
SET logsql_table = 'logs_05042';
SET dialect = 'logsql';

-- Comparison filters accept the numeric spellings in any case, on String and typed columns alike.
val:<INF | count();
val:<=+Infinity | count();
val:>=-INF | count();
val:=NaN | count();
size:<=INF | count();
price:=NaN | count();

-- Ordinary words that merely start like a numeric spelling stay textual (string comparison).
val:>info | count();

-- The <DATETIME> timezone offset is exactly +HH:MM / -HH:MM or Z; "+123:456" is not an offset.
_msg:pattern_match_full("<DATETIME>") | count();

-- Text-style pipes operate on the LogsQL string value of typed fields.
* | len(size) as l | fields l | sort by (l);
* | len(note) as l | fields l | sort by (l);
* | coalesce(note, size) as c | fields c | sort by (c);
* | replace ("2", "9") at size | fields size | sort by (size);
* | replace_regexp ("[0-9]+", "N") at price | fields price | sort by (price);
* | split "." from price as parts | fields parts | sort by (parts);
* | unpack_words size words | fields words | sort by (words);
* | decolorize size | fields size | sort by (size);
* | json_array_len(size) as n | fields n | sort by (n);

-- hash() hashes the field text, so a typed field and its text hash identically.
* | hash(size) as h1 | format "<size>" as s | hash(s) as h2 | math h1 - h2 as delta | fields delta | sort by (delta);

SET dialect = 'clickhouse';
DROP TABLE logs_05042;
