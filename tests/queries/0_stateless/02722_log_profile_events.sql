-- There are no fatal errors:
SELECT count() FROM system.events WHERE event = 'LogFatal';

-- It counts the trace log messages:
SELECT count() > 0 FROM system.events WHERE event = 'LogTrace';
