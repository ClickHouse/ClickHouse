SELECT formatDateTime(toDateTime('2023-08-25 15:30:00', 'UTC'), '%H:%M:%S', toLowCardinality('America/Los_Angeles'));
SELECT formatDateTime(toDateTime('2023-08-25 15:30:00', 'UTC'), '%H:%M:%S', toLowCardinality('Europe/Amsterdam'));
SELECT formatDateTime(materialize(toDateTime('2023-08-25 15:30:00', 'UTC')), '%H:%M:%S', toLowCardinality('Europe/Amsterdam'));
SELECT toString(toDateTime('2023-08-25 15:30:00', 'UTC'), toLowCardinality('Europe/Amsterdam'));
