SET output_format_pretty_display_footer_column_names = 0;
SET output_format_pretty_color = 1;
SET output_format_pretty_highlight_json = 1;
SET output_format_pretty_fallback_to_vertical = 0;

DROP TABLE IF EXISTS t_json_highlight;
CREATE TABLE t_json_highlight (d JSON) ENGINE = Memory;

INSERT INTO t_json_highlight VALUES ('{"a": "hello", "b": 42, "c": {"nested": true}}');

SELECT * FROM t_json_highlight FORMAT Pretty;
SELECT * FROM t_json_highlight FORMAT PrettyCompact;
SELECT * FROM t_json_highlight FORMAT Vertical;

DROP TABLE t_json_highlight;
