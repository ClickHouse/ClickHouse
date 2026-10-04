CREATE TABLE top_column_source (`TOP` String) ENGINE = MergeTree() ORDER BY `TOP`;

CREATE VIEW top_column_view (c String) AS
WITH a AS
(
    SELECT `TOP` AS c
    FROM top_column_source
)
SELECT c
FROM a;

SHOW CREATE VIEW top_column_view FORMAT Null;

DROP VIEW top_column_view;
DROP TABLE top_column_source;
