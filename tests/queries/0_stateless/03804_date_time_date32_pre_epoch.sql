-- { echoOn }

SELECT 'Original bug case:';
SELECT date_trunc('day', toDate32('1911-01-01'));

SELECT 'date_trunc with pre-1970 Date32:';
SELECT date_trunc('year', toDate32('1911-01-01'));
SELECT date_trunc('quarter', toDate32('1911-01-01'));
SELECT date_trunc('month', toDate32('1911-01-01'));
SELECT date_trunc('week', toDate32('1911-01-01'));
SELECT date_trunc('day', toDate32('1911-01-01'));

SELECT 'Dates just before epoch:';
SELECT date_trunc('year', toDate32('1969-12-31'));
SELECT date_trunc('quarter', toDate32('1969-12-31'));
SELECT date_trunc('month', toDate32('1969-12-31'));
SELECT date_trunc('week', toDate32('1969-12-31'));
SELECT date_trunc('day', toDate32('1969-12-31'));

SELECT 'Epoch date:';
SELECT date_trunc('year', toDate32('1970-01-01'));
SELECT date_trunc('quarter', toDate32('1970-01-01'));
SELECT date_trunc('month', toDate32('1970-01-01'));
SELECT date_trunc('week', toDate32('1970-01-01'));
SELECT date_trunc('day', toDate32('1970-01-01'));

SELECT 'Date just after epoch:';
SELECT date_trunc('year', toDate32('1970-01-02'));
SELECT date_trunc('quarter', toDate32('1970-01-02'));
SELECT date_trunc('month', toDate32('1970-01-02'));
SELECT date_trunc('week', toDate32('1970-01-02'));
SELECT date_trunc('day', toDate32('1970-01-02'));

SELECT 'Near Date32 minimum:';
SELECT date_trunc('year', toDate32('1900-01-01'));
SELECT date_trunc('quarter', toDate32('1900-01-01'));
SELECT date_trunc('month', toDate32('1900-01-01'));
SELECT date_trunc('week', toDate32('1900-01-01'));
SELECT date_trunc('day', toDate32('1900-01-01'));

SELECT 'Mid-year pre-1970 dates:';
SELECT date_trunc('year', toDate32('1950-06-15'));
SELECT date_trunc('quarter', toDate32('1950-06-15'));
SELECT date_trunc('month', toDate32('1950-06-15'));
SELECT date_trunc('week', toDate32('1950-06-15'));
SELECT date_trunc('day', toDate32('1950-06-15'));

SELECT 'Return types for Date32 input:';
SELECT toTypeName(date_trunc('year', toDate32('1911-01-01')));
SELECT toTypeName(date_trunc('quarter', toDate32('1911-01-01')));
SELECT toTypeName(date_trunc('month', toDate32('1911-01-01')));
SELECT toTypeName(date_trunc('week', toDate32('1911-01-01')));
SELECT toTypeName(date_trunc('day', toDate32('1911-01-01')));

SELECT 'Comparison Date32 vs Date for post-1970:';
SELECT date_trunc('day', toDate32('2022-09-16')) = date_trunc('day', toDate('2022-09-16'));
SELECT date_trunc('week', toDate32('2022-09-16')) = date_trunc('week', toDate('2022-09-16'));
SELECT date_trunc('month', toDate32('2022-09-16')) = date_trunc('month', toDate('2022-09-16'));
SELECT date_trunc('quarter', toDate32('2022-09-16')) = date_trunc('quarter', toDate('2022-09-16'));
SELECT date_trunc('year', toDate32('2022-09-16')) = date_trunc('year', toDate('2022-09-16'));
