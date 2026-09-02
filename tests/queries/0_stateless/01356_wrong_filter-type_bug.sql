drop table if exists t0;

CREATE TABLE t0 (`c0` String, `c1` Int32 CODEC(NONE), `c2` Int32) ENGINE = MergeTree() ORDER BY tuple();
insert into t0 values ('a', 1, 2);

SELECT t0.c2, t0.c1, t0.c0 FROM t0 PREWHERE t0.c0 ORDER BY ((t0.c2)>=(t0.c1)), (((- (((t0.c0)>(t0.c0))))) IS NULL) FORMAT TabSeparatedWithNamesAndTypes; -- {serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER}
SELECT t0.c2, t0.c1, t0.c0 FROM t0 WHERE t0.c0 ORDER BY ((t0.c2)>=(t0.c1)), (((- (((t0.c0)>(t0.c0))))) IS NULL) FORMAT TabSeparatedWithNamesAndTypes settings optimize_move_to_prewhere=0; -- {serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER}

drop table if exists t0;
