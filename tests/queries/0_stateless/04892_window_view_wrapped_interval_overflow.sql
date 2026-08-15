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

-- A sane window is unaffected.
CREATE WINDOW VIEW 04892_wv ENGINE = Memory WATERMARK = STRICTLY_ASCENDING
    AS SELECT count(v) AS c, tumble(ts, toIntervalHour(1), 'UTC') AS w FROM 04892_src GROUP BY w;

INSERT INTO 04892_src VALUES ('2026-08-13 10:00:00', 1);
INSERT INTO 04892_src VALUES ('2026-08-13 12:00:00', 1);

SELECT count() FROM 04892_src;

DROP TABLE 04892_wv;
DROP TABLE 04892_src;
