-- The tag `__name__.dropped` is reserved. Insert and `tags_to_columns` reject it.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_reserved_tag;

CREATE TABLE ts_reserved_tag ENGINE = TimeSeries;

INSERT INTO ts_reserved_tag (metric_name, tags, samples) VALUES ('foo', {'__name__.dropped': '1'}, [(toDateTime64(1000, 3), 1)]); -- { serverError ILLEGAL_TIME_SERIES_TAGS }

CREATE TABLE ts_reserved_tag_columns ENGINE = TimeSeries SETTINGS tags_to_columns = {'__name__.dropped': 'x'}; -- { serverError INVALID_SETTING_VALUE }

DROP TABLE ts_reserved_tag;
