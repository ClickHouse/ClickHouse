DROP TABLE IF EXISTS test.local_statements;
DROP TABLE IF EXISTS test.statements;

CREATE TABLE test.local_statements ( statementId String, eventDate Date, eventHour DateTime, eventTime DateTime, verb String, objectId String, onCourse UInt8, courseId UInt16, contextRegistration String, resultScoreRaw Float64, resultScoreMin Float64, resultScoreMax Float64, resultSuccess UInt8, resultCompletition UInt8, resultDuration UInt32, resultResponse String, learnerId String, learnerHash String, contextId UInt16) ENGINE = MergeTree ORDER BY tuple();

CREATE TABLE test.statements ( statementId String, eventDate Date, eventHour DateTime, eventTime DateTime, verb String, objectId String, onCourse UInt8, courseId UInt16, contextRegistration String, resultScoreRaw Float64, resultScoreMin Float64, resultScoreMax Float64, resultSuccess UInt8, resultCompletition UInt8, resultDuration UInt32, resultResponse String, learnerId String, learnerHash String, contextId UInt16) ENGINE = Distributed(test_shard_localhost, 'test', 'local_statements', sipHash64(learnerHash));

INSERT INTO test.local_statements FORMAT CSV "2b3b04ee-0bb8-4200-906f-d47c48e56bd0","2016-08-25","2016-08-25 14:00:00","2016-08-25 14:43:34","http://adlnet.gov/expapi/verbs/passed","https://crmm.ru/xapi/courses/spp/2/0/3/2/8",0,1,"c13d788c-26e0-40e3-bacb-a1ff78ee1518",100,0,0,0,0,0,"","https://sberbank-school.ru/xapi/accounts/userid/94312","6f696f938a69b5e173093718e1c2bbf2",0

SELECT avg(diff)
FROM
(
    SELECT *
    FROM
    (
        SELECT
            learnerHash,
            passed - eventTime AS diff
        FROM test.statements
        GLOBAL ANY INNER JOIN
        (
            SELECT
                learnerHash,
                argMax(eventTime, resultScoreRaw) AS passed
            FROM
            (
                SELECT
                    learnerHash,
                    eventTime,
                    resultScoreRaw
                FROM test.statements
                WHERE (courseId = 1) AND (onCourse = 0)
                    AND (verb = 'http://adlnet.gov/expapi/verbs/passed') AND (objectId = 'https://crmm.ru/xapi/courses/spp/1/1/0-1')
                ORDER BY eventTime ASC
            )
            GROUP BY learnerHash
        ) USING (learnerHash)
        WHERE (courseId = 1) AND (onCourse = 0)
            AND (verb = 'http://adlnet.gov/expapi/verbs/interacted') AND (eventTime <= passed) AND (diff > 0)
        ORDER BY eventTime DESC
        LIMIT 1 BY learnerHash
    )
    ORDER BY diff DESC
    LIMIT 7, 126
);

DROP TABLE test.local_statements;
DROP TABLE test.statements;
