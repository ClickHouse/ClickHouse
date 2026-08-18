-- A `WINDOW VIEW` advances its window bounds - the watermark and the next fire signal - in loops
-- which assume that adding a positive interval moves the time. An interval whose span in seconds is
-- a multiple of 2^32, such as `INTERVAL 2147483648 DAY`, does not move the time at all in the
-- wrapping `UInt32` arithmetic, so the loop firing the windows of a processing time window view used
-- to spin forever in a background thread, holding the mutex of the window view.
-- Such a window is not representable and is rejected at creation.
-- https://github.com/ClickHouse/ClickHouse/issues/114605

SET allow_experimental_window_view = 1;
SET allow_experimental_analyzer = 0;

DROP TABLE IF EXISTS 04892_wv;
DROP TABLE IF EXISTS 04892_src;

CREATE TABLE 04892_src (ts DateTime('UTC'), v UInt64) ENGINE = Memory;

CREATE WINDOW VIEW 04892_wv ENGINE = Memory
    AS SELECT count(v) AS c, tumble(now(), toIntervalDay(2147483648), 'UTC') AS w FROM 04892_src GROUP BY w; -- { serverError BAD_ARGUMENTS }

-- Intervals that wrap to a different `UInt32` value are rejected as well.
CREATE WINDOW VIEW 04892_wv ENGINE = Memory
    AS SELECT count(v) AS c, tumble(now(), toIntervalDay(2147483647), 'UTC') AS w FROM 04892_src GROUP BY w; -- { serverError BAD_ARGUMENTS }

CREATE WINDOW VIEW 04892_wv ENGINE = Memory WATERMARK = STRICTLY_ASCENDING
    AS SELECT count(v) AS c, tumble(ts, toIntervalDay(2147483648), 'UTC') AS w FROM 04892_src GROUP BY w; -- { serverError BAD_ARGUMENTS }

-- The largest representable day interval still produces a valid first window for all
-- `DateTime32` timestamps.
CREATE WINDOW VIEW 04892_wv ENGINE = Memory WATERMARK = STRICTLY_ASCENDING
    AS SELECT count(v) AS c, tumble(ts, toIntervalDay(49710), 'UTC') AS w FROM 04892_src GROUP BY w;

INSERT INTO 04892_src VALUES ('2106-02-07 05:00:00', 1);

DROP TABLE 04892_wv;
TRUNCATE TABLE 04892_src;

-- The window size of a hopping window is checked as well.
CREATE WINDOW VIEW 04892_wv ENGINE = Memory
    AS SELECT count(v) AS c, hop(now(), toIntervalDay(1), toIntervalDay(2147483648), 'UTC') AS w FROM 04892_src GROUP BY w; -- { serverError BAD_ARGUMENTS }

-- Watermark and lateness intervals have the same requirement as window intervals.
CREATE WINDOW VIEW 04892_wv ENGINE = Memory WATERMARK = INTERVAL 2147483648 DAY
    AS SELECT count(v) AS c, tumble(ts, toIntervalHour(1), 'UTC') AS w FROM 04892_src GROUP BY w; -- { serverError BAD_ARGUMENTS }

CREATE WINDOW VIEW 04892_wv ENGINE = Memory ALLOWED_LATENESS INTERVAL 2147483648 DAY
    AS SELECT count(v) AS c, tumble(now(), toIntervalHour(1), 'UTC') AS w FROM 04892_src GROUP BY w; -- { serverError BAD_ARGUMENTS }

-- `ATTACH` rejects unsafe legacy bounded-watermark and lateness metadata as well: a wrapped
-- watermark would otherwise throw from the noexcept `WatermarkTransform` destructor and wrapped
-- lateness would silently be treated as zero.
ATTACH WINDOW VIEW 04892_wv ENGINE = Memory WATERMARK = INTERVAL 2147483648 DAY
    AS SELECT count(v) AS c, tumble(ts, toIntervalHour(1), 'UTC') AS w FROM 04892_src GROUP BY w; -- { serverError BAD_ARGUMENTS }

ATTACH WINDOW VIEW 04892_wv ENGINE = Memory ALLOWED_LATENESS INTERVAL 2147483648 DAY
    AS SELECT count(v) AS c, tumble(now(), toIntervalHour(1), 'UTC') AS w FROM 04892_src GROUP BY w; -- { serverError BAD_ARGUMENTS }

-- A bounded watermark that is representable at the epoch can still overflow when it is applied to
-- a timestamp near the end of `DateTime32`. The view must not terminate the server while handling
-- the invalid bound.
CREATE WINDOW VIEW 04892_wv ENGINE = Memory WATERMARK = INTERVAL 2147483647 SECOND
    AS SELECT count(v) AS c, tumble(ts, toIntervalHour(1), 'UTC') AS w FROM 04892_src GROUP BY w;

INSERT INTO 04892_src VALUES ('2106-02-07 05:00:00', 1);

DROP TABLE 04892_wv;

TRUNCATE TABLE 04892_src;

-- Likewise, subtracting a valid lateness interval from early timestamps must not wrap and cause
-- the filter to silently discard all rows or terminate the server.
CREATE WINDOW VIEW 04892_wv ENGINE = Memory WATERMARK = STRICTLY_ASCENDING ALLOWED_LATENESS INTERVAL 1 DAY
    AS SELECT count(v) AS c, tumble(ts, toIntervalHour(1), 'UTC') AS w FROM 04892_src GROUP BY w;

INSERT INTO 04892_src VALUES ('1970-01-01 00:00:01', 1);

DROP TABLE 04892_wv;

TRUNCATE TABLE 04892_src;

-- A sane window is unaffected.
CREATE WINDOW VIEW 04892_wv ENGINE = Memory WATERMARK = STRICTLY_ASCENDING
    AS SELECT count(v) AS c, tumble(ts, toIntervalHour(1), 'UTC') AS w FROM 04892_src GROUP BY w;

INSERT INTO 04892_src VALUES ('2026-08-13 10:00:00', 1);
INSERT INTO 04892_src VALUES ('2026-08-13 12:00:00', 1);

SELECT count() FROM 04892_src;

DROP TABLE 04892_wv;
DROP TABLE 04892_src;
