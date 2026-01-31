-- Tags: long, no-parallel, no-fasttest, no-replicated-database
-- Tag no-fasttest: Depends on S3
-- Tag no-replicated-database: plain rewritable should not be shared between replicas
-- Tag no-parallel: self-concurrency in flaky check (pre-configured RW disk cannot be
--                  shared across the tables from different test instances)

SET alter_sync = 2,
    mutations_sync = 2;

DROP TABLE IF EXISTS 03787_data SYNC;

CREATE TABLE 03787_data (key Int32, val String) ENGINE = MergeTree() ORDER BY key
SETTINGS disk = 'disk_s3_plain_rewritable_03787';

INSERT INTO 03787_data SELECT number, 'val-' || number FROM numbers(10);

SELECT '-- Initial data';
SELECT * FROM 03787_data ORDER BY key FORMAT TabSeparatedWithNamesAndTypes;

SELECT '-- Adding Date column';
ALTER TABLE 03787_data ADD COLUMN dt Date;
SELECT * FROM 03787_data ORDER BY key FORMAT TabSeparatedWithNamesAndTypes;

SELECT '-- Updating Date column';
ALTER TABLE 03787_data UPDATE dt = toDate('2025-01-01') + key WHERE 1 = 1;
SELECT * FROM 03787_data ORDER BY key FORMAT TabSeparatedWithNamesAndTypes;

SELECT '-- Dropping String column';
ALTER TABLE 03787_data DROP COLUMN val;
SELECT * FROM 03787_data ORDER BY key FORMAT TabSeparatedWithNamesAndTypes;

SELECT '-- Changing type of Date column';
ALTER TABLE 03787_data MODIFY COLUMN dt UInt128;
SELECT * FROM 03787_data ORDER BY key FORMAT TabSeparatedWithNamesAndTypes;

SELECT '-- Renaming column';
ALTER TABLE 03787_data RENAME COLUMN dt TO renamed;
SELECT * FROM 03787_data ORDER BY key FORMAT TabSeparatedWithNamesAndTypes;

SELECT '-- Clearing column';
ALTER TABLE 03787_data CLEAR COLUMN renamed;
SELECT * FROM 03787_data ORDER BY key FORMAT TabSeparatedWithNamesAndTypes;

DROP TABLE 03787_data SYNC;
