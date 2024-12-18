-- Tags: long, no-object-storage, no-asan

SET joined_subquery_requires_alias = 0;

-- We are no longer interested in the old analyzer.
SET allow_experimental_analyzer = 1;

-- This test (SELECT) without cache can take tens minutes
DROP TABLE IF EXISTS dict_string;
DROP TABLE IF EXISTS dict_ui64;
DROP TABLE IF EXISTS video_views;

set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE video_views
(
    entityIri String,
    courseId UInt64,
    learnerId UInt64,
    actorId UInt64,
    duration UInt16,
    fullWatched UInt8,
    fullWatchedDate DateTime,
    fullWatchedDuration UInt16,
    fullWatchedTime UInt16,
    fullWatchedViews UInt16,
    `views.viewId` Array(String),
    `views.startedAt` Array(DateTime),
    `views.endedAt` Array(DateTime),
    `views.viewDuration` Array(UInt16),
    `views.watchedPart` Array(Float32),
    `views.fullWatched` Array(UInt8),
    `views.progress` Array(Float32),
    `views.reject` Array(UInt8),
    `views.viewNumber` Array(UInt16),
    `views.repeatingView` Array(UInt8),
    `views.ranges` Array(String),
    version DateTime
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY entityIri
ORDER BY (learnerId, entityIri)
SETTINGS index_granularity = 8192;

CREATE TABLE dict_string (entityIri String) ENGINE = Memory;
CREATE TABLE dict_ui64 (learnerId UInt64) ENGINE = Memory;

SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average`, `views-duration-average`, `watched-part-average`, `rejects-count`, `progress-average`, `views-count-before-full-watched-average`, `duration`, `full-watched-learners-count`, `overall-watchers-count`, `overall-full-watched-learners-count`, `views-count`, `time-before-full-watched-average`, if (isNaN((`overall-full-watched-learners-count`/`overall-watchers-count`) * 100), 0, (`overall-full-watched-learners-count`/`overall-watchers-count`) * 100) as `overall-watched-part`, if (isNaN((`full-watched-learners-count`/`watchers-count` * 100)), 0, (`full-watched-learners-count`/`watchers-count` * 100)) as `full-watched-part`, if (isNaN((`rejects-count`/`views-count` * 100)), 0, (`rejects-count`/`views-count` * 100)) as `rejects-part` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average`, `views-duration-average`, `watched-part-average`, `rejects-count`, `progress-average`, `views-count-before-full-watched-average`, `duration`, `full-watched-learners-count`, `overall-watchers-count`, `overall-full-watched-learners-count`, `views-count`, `time-before-full-watched-average` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average`, `views-duration-average`, `watched-part-average`, `rejects-count`, `progress-average`, `views-count-before-full-watched-average`, `duration`, `full-watched-learners-count`, `overall-watchers-count`, `overall-full-watched-learners-count`, `views-count` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average`, `views-duration-average`, `watched-part-average`, `rejects-count`, `progress-average`, `views-count-before-full-watched-average`, `duration`, `full-watched-learners-count`, `overall-watchers-count`, `overall-full-watched-learners-count` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average`, `views-duration-average`, `watched-part-average`, `rejects-count`, `progress-average`, `views-count-before-full-watched-average`, `duration`, `full-watched-learners-count`, `overall-watchers-count` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average`, `views-duration-average`, `watched-part-average`, `rejects-count`, `progress-average`, `views-count-before-full-watched-average`, `duration`, `full-watched-learners-count` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average`, `views-duration-average`, `watched-part-average`, `rejects-count`, `progress-average`, `views-count-before-full-watched-average`, `duration` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average`, `views-duration-average`, `watched-part-average`, `rejects-count`, `progress-average`, `views-count-before-full-watched-average` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average`, `views-duration-average`, `watched-part-average`, `rejects-count`, `progress-average` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average`, `views-duration-average`, `watched-part-average`, `rejects-count` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average`, `views-duration-average`, `watched-part-average` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average`, `repeating-views-count-average`, `views-duration-average` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`,
 `reject-views-duration-average`, `repeating-views-count-average` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average`, `reject-views-duration-average` FROM (SELECT `entityIri`, `watchers-count`, `time-repeating-average` FROM (SELECT `entityIri`, `watchers-count` FROM (SELECT `entityIri` FROM dict_string) ANY LEFT JOIN (SELECT uniq(learnerId) as `watchers-count`, `entityIri` FROM `video_views` FINAL ARRAY JOIN `views` PREWHERE `entityIri` IN dict_string AND `courseId` = 1 WHERE `learnerId` IN dict_ui64 GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT avg(viewDurationSum) as `time-repeating-average`, `entityIri` FROM (SELECT sum(views.viewDuration) as viewDurationSum, `entityIri`, `learnerId` FROM `video_views` FINAL ARRAY JOIN `views` PREWHERE `entityIri` IN dict_string AND `courseId` = 1 WHERE `views`.`repeatingView` = 1 AND `learnerId` IN dict_ui64 GROUP BY `learnerId`, `entityIri`) GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT avg(views.viewDuration) as `reject-views-duration-average`, `entityIri` FROM `video_views` FINAL ARRAY JOIN `views` PREWHERE `entityIri` IN dict_string AND `courseId` = 1 WHERE `views`.`reject` = 1 AND `learnerId` IN dict_ui64 GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT avg(viewsCount) as `repeating-views-count-average`, `entityIri` FROM (SELECT count() as viewsCount, `learnerId`, `entityIri` FROM `video_views` FINAL ARRAY JOIN `views` PREWHERE `courseId` = 1 AND `entityIri` IN dict_string WHERE `views`.`repeatingView` = 1 AND `learnerId` IN dict_ui64 GROUP BY `learnerId`, `entityIri`) GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT avg(views.viewDuration) as `views-duration-average`, `entityIri` FROM `video_views` FINAL ARRAY JOIN `views` PREWHERE `entityIri` IN dict_string AND `courseId` = 1 WHERE `learnerId` IN dict_ui64 GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT avg(views.watchedPart) as `watched-part-average`, `entityIri` FROM `video_views` FINAL ARRAY JOIN `views` PREWHERE `entityIri` IN dict_string AND `courseId` = 1 WHERE `learnerId` IN dict_ui64 GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT count() as `rejects-count`, `entityIri` FROM `video_views` FINAL ARRAY JOIN `views` PREWHERE `entityIri` IN dict_string AND `courseId` = 1 WHERE `views`.`reject` = 1 AND `learnerId` IN dict_ui64 GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT avg(progressMax) as `progress-average`, `entityIri` FROM (SELECT max(views.progress) as progressMax, `entityIri`, `learnerId` FROM `video_views` FINAL ARRAY JOIN `views` PREWHERE `entityIri` IN dict_string AND `courseId` = 1 WHERE `learnerId` IN dict_ui64 GROUP BY `learnerId`, `entityIri`) GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT avg(fullWatchedViews) as `views-count-before-full-watched-average`, `entityIri` FROM `video_views` FINAL PREWHERE `entityIri` IN dict_string AND `courseId` = 1 WHERE `learnerId` IN dict_ui64 GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT any(duration) as `duration`, `entityIri` FROM `video_views` FINAL PREWHERE `entityIri` IN dict_string AND `courseId` = 1 WHERE `learnerId` IN dict_ui64 GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT uniq(learnerId) as `full-watched-learners-count`, `entityIri` FROM `video_views` FINAL PREWHERE `entityIri` IN dict_string AND `courseId` = 1 WHERE `fullWatched` = 1 AND `learnerId` IN dict_ui64 GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT uniq(learnerId) as `overall-watchers-count`, `entityIri` FROM `video_views` FINAL ARRAY JOIN `views` PREWHERE `entityIri` IN dict_string AND `courseId` = 1 WHERE `learnerId` IN dict_ui64 GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT uniq(learnerId) as `overall-full-watched-learners-count`,
  `entityIri` FROM `video_views` FINAL PREWHERE `entityIri` IN dict_string AND `courseId` = 1 WHERE `fullWatched` = 1 AND `learnerId` IN dict_ui64 GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT count() as `views-count`, `entityIri` FROM `video_views` FINAL ARRAY JOIN `views` PREWHERE `entityIri` IN dict_string AND `courseId` = 1 WHERE `learnerId` IN dict_ui64 GROUP BY `entityIri`) USING `entityIri`) ANY LEFT JOIN (SELECT avg(fullWatchedTime) as `time-before-full-watched-average`, `entityIri` FROM `video_views` FINAL PREWHERE `entityIri` IN dict_string AND `courseId` = 1 WHERE `learnerId` IN dict_ui64 GROUP BY `entityIri`) USING `entityIri`);

SELECT 'Still alive';

DROP TABLE dict_string;
DROP TABLE dict_ui64;
DROP TABLE video_views;



-- Test for tsan: Ensure cache is used from one thread
SET max_threads = 32, max_memory_usage = '10G';

DROP TABLE IF EXISTS sample_00632;

CREATE TABLE sample_00632 (d Date DEFAULT '2000-01-01', x UInt16) ENGINE = MergeTree(d, x, x, 10);
INSERT INTO sample_00632 (x) SELECT toUInt16(number) AS x FROM system.numbers LIMIT 65536;

SELECT count()
FROM
(
    SELECT
        x,
        count() AS c
    FROM
    (
                  SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
        UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
        UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
        UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
        UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
        UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
        UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
        UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
        UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
        UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
        UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
        UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    UNION ALL SELECT * FROM ( SELECT * FROM sample_00632 WHERE x > 0 )
    )
    GROUP BY x
    ORDER BY x ASC
);
DROP TABLE sample_00632;
