-- Bug #71775
-- An explicit time zone keeps the (overflowing) result deterministic regardless of the session time zone.
SELECT toStartOfNanosecond('2263-01-01 00:00:00'::DateTime64(3, 'UTC'));
