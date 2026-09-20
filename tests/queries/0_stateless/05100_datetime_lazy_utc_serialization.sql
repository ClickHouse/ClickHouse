SET session_timezone = 'Asia/Kolkata';
SET input_format_values_interpret_expressions = 0;
SET date_time_input_format = 'basic';

DROP TABLE IF EXISTS datetime_lazy_utc_serialization;
CREATE TABLE datetime_lazy_utc_serialization
(
    label String,
    dt DateTime('Asia/Kolkata'),
    dt64 DateTime64(3, 'Asia/Kolkata')
)
ENGINE = Memory;

INSERT INTO datetime_lazy_utc_serialization FORMAT CSV
"basic_csv_quoted","1970-01-02 05:30:00","1970-01-02 05:30:00.125"
basic_csv_unquoted,1970-01-02 05:30:00,1970-01-02 05:30:00.125

SET date_time_input_format = 'best_effort';

INSERT INTO datetime_lazy_utc_serialization FORMAT CSV
"best_effort_csv_quoted","1970-01-02T00:00:00Z","1970-01-02T00:00:00.125Z"
best_effort_csv_unquoted,1970-01-02T03:30:00+03:30,1970-01-02T03:30:00.125+03:30

INSERT INTO datetime_lazy_utc_serialization FORMAT JSONEachRow
{"label":"best_effort_json","dt":"1970-01-02T00:00:00Z","dt64":"1970-01-02T00:00:00.125Z"}

INSERT INTO datetime_lazy_utc_serialization FORMAT TabSeparated
best_effort_tsv	1970-01-02T00:00:00Z	1970-01-02T00:00:00.125Z

INSERT INTO datetime_lazy_utc_serialization VALUES ('best_effort_values', '1970-01-02T00:00:00Z', '1970-01-02T00:00:00.125Z');

SET date_time_input_format = 'best_effort_us';

INSERT INTO datetime_lazy_utc_serialization FORMAT JSONEachRow
{"label":"best_effort_us_json","dt":"01/02/1970 00:00:00+0000","dt64":"01/02/1970 00:00:00.125+0000"}

INSERT INTO datetime_lazy_utc_serialization FORMAT TabSeparated
best_effort_us_tsv	01/02/1970 00:00:00+0000	01/02/1970 00:00:00.125+0000

INSERT INTO datetime_lazy_utc_serialization VALUES ('best_effort_us_values', '01/02/1970 00:00:00+0000', '01/02/1970 00:00:00.125+0000');

SELECT label, toUnixTimestamp(dt), toUnixTimestamp64Milli(dt64)
FROM datetime_lazy_utc_serialization
ORDER BY label;

SET date_time_output_format = 'simple';
SELECT dt, dt64 FROM datetime_lazy_utc_serialization WHERE label = 'basic_csv_quoted' FORMAT CSV;

SET date_time_output_format = 'iso';
SELECT dt, dt64 FROM datetime_lazy_utc_serialization WHERE label = 'basic_csv_quoted' FORMAT CSV;

SET date_time_output_format = 'unix_timestamp';
SELECT dt, dt64 FROM datetime_lazy_utc_serialization WHERE label = 'basic_csv_quoted' FORMAT CSV;

SELECT hex(formatRowNoNewline('RowBinary', dt, dt64)) FROM datetime_lazy_utc_serialization WHERE label = 'basic_csv_quoted';

DROP TABLE datetime_lazy_utc_serialization;
