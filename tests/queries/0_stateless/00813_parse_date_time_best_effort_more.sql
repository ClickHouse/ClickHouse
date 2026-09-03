SET output_format_pretty_display_footer_column_names=0;
SELECT
    s,
    parseDateTimeBestEffortOrNull(s, 'UTC') AS a,
    parseDateTimeBestEffortOrZero(s, 'UTC') AS b
FROM
(
    SELECT arrayJoin([
'24.12.2018',
'24-12-2018',
'24.12.18',
'24-12-18',
'24-Dec-18',
'24/DEC/18',
'24/DEC/2018',
'01-OCT-2015',
'24.12.2018',
'24-12-2018',
'24.12.18',
'24-12-18',
'24-Dec-18',
'24/DEC/18',
'24/DEC/2018',
'01-OCT-2015',
'24.12.18 010203',
'24.12.18 01:02:03',
'24.DEC.18T01:02:03.000+0300',
'01-September-2018 11:22'
]) AS s)
FORMAT PrettySpaceNoEscapes;
