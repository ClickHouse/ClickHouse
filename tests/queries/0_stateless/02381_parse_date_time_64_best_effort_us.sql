SELECT 'parseDateTime64BestEffortUS';

SELECT
    s,
    parseDateTime64BestEffortUS(s) AS a
FROM
(
    SELECT arrayJoin([
'01-02-1930 12:00:00',
'12.02.1930 12:00:00',
'13/02/1930 12:00:00',
'02/25/1930 12:00:00'
]) AS s)
FORMAT PrettySpaceNoEscapes;

SELECT '';

SELECT 'parseDateTime64BestEffortUSOrNull';
SELECT parseDateTime64BestEffortUSOrNull('01/45/1925 16:00:00');

SELECT 'parseDateTime64BestEffortUSOrZero';
SELECT parseDateTime64BestEffortUSOrZero('01/45/1925 16:00:00');
