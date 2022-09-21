set query_plan_filter_push_down=0;

CREATE TABLE test_t
(
    `StartDate` Date,
    `PublisherEvents.Authors` Array(Array(String))
)
ENGINE = Log;


SELECT `PubAuth.Author`
FROM
(
	SELECT
	    StartDate,
	    `Pub.Authors`
	FROM test_t
	LEFT ARRAY JOIN `PublisherEvents.Authors` AS `Pub.Authors`
	WHERE StartDate = today()
)
ARRAY JOIN `Pub.Authors` AS `PubAuth.Author`
WHERE StartDate = today();

