DROP TABLE IF EXISTS time_decay_view_source;
DROP VIEW IF EXISTS time_decay_view;
DROP VIEW IF EXISTS time_decay_parameterized_view;

SET allow_experimental_time_decay_aggregate_functions = 1;
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE time_decay_view_source
(
    value ExponentialTimeDecayingFloat64(10),
    ordinary LowCardinality(UInt8)
)
ENGINE = Memory;
INSERT INTO time_decay_view_source VALUES ((1., 0., 10.), 7);
SET allow_suspicious_low_cardinality_types = 0;

-- Ordinary views are exempt from storage-specific gates, while the time-decay gate remains active.
CREATE VIEW time_decay_view AS SELECT ordinary FROM time_decay_view_source;
SELECT ordinary FROM time_decay_view;
DROP VIEW time_decay_view;
CREATE VIEW time_decay_view AS SELECT value FROM time_decay_view_source;
SELECT exponentialTimeDecayingValueAt(value, 0) FROM time_decay_view;
DROP VIEW time_decay_view;

SET allow_experimental_time_decay_aggregate_functions = 0;
CREATE VIEW time_decay_view AS SELECT value FROM time_decay_view_source; -- { serverError ILLEGAL_COLUMN }
CREATE VIEW time_decay_parameterized_view AS SELECT {value:ExponentialTimeDecayingFloat64(10)}; -- { serverError ILLEGAL_COLUMN }
ATTACH TABLE time_decay_attached (value ExponentialTimeDecayingFloat64(10)) ENGINE = Memory; -- { serverError ILLEGAL_COLUMN }

DROP TABLE time_decay_view_source;
