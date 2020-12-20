SELECT parseDateTimeBestEffortOrNull('2010-01-01');
SELECT parseDateTimeBestEffortOrNull('2010-01-01 01:01:01');
SELECT parseDateTimeBestEffortOrNull('01:01:01');
SELECT parseDateTimeBestEffortOrNull('20100');
SELECT parseDateTimeBestEffortOrNull('0100:0100:0000');
SELECT parseDateTimeBestEffortOrNull('x');
SELECT parseDateTimeBestEffortOrNull('');
SELECT parseDateTimeBestEffortOrNull('       ');
