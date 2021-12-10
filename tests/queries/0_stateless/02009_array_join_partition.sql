CREATE TABLE table_2009_part (`i` Int64, `d` Date, `s` String) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY i;

ALTER TABLE table_2009_part ATTACH PARTITION tuple(arrayJoin([0, 1])); -- {serverError 36}
ALTER TABLE table_2009_part ATTACH PARTITION tuple(toYYYYMM(toDate([arrayJoin([arrayJoin([arrayJoin([arrayJoin([3, materialize(NULL), arrayJoin([1025, materialize(NULL), materialize(NULL)]), NULL])])]), materialize(NULL)])], NULL))); -- {serverError 36}
