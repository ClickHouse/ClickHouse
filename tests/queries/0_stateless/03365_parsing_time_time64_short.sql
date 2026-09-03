SET use_legacy_to_time = 0;

-- Regular Time formats (MM:SS)
SELECT toTime('45:30');
SELECT toTime('00:01');
SELECT toTime('59:59');

-- Single digit minutes (M:SS)
SELECT toTime('5:30');
SELECT toTime('9:59');
SELECT toTime('0:01');

-- Seconds only (SS)
SELECT toTime('45');
SELECT toTime('01');
SELECT toTime('59');

-- Single digit seconds (S)
SELECT toTime('5');
SELECT toTime('9');
SELECT toTime('0');

-- Negative values for Time (MM:SS)
SELECT toTime('-45:30');
SELECT toTime('-00:01');
SELECT toTime('-59:59');

-- Negative single digit minutes (M:SS)
SELECT toTime('-5:30');
SELECT toTime('-9:59');
SELECT toTime('-0:01');

-- Negative seconds only (SS)
SELECT toTime('-45');
SELECT toTime('-01');
SELECT toTime('-59');

-- Negative single digit seconds (S)
SELECT toTime('-5');
SELECT toTime('-9');
SELECT toTime('-0');

-- Edge cases
SELECT toTime('99:99'); -- Beyond normal time limits but should parse

-- Time64 formats with precision 0 (MM:SS)
SELECT toTime64('45:30', 0);
SELECT toTime64('00:01', 0);
SELECT toTime64('59:59', 0);

-- Time64 formats with precision 0 (M:SS)
SELECT toTime64('5:30', 0);
SELECT toTime64('9:59', 0);
SELECT toTime64('0:01', 0);

-- Time64 formats with precision 0 (SS)
SELECT toTime64('45', 0);
SELECT toTime64('01', 0);
SELECT toTime64('59', 0);

-- Time64 formats with precision 0 (S)
SELECT toTime64('5', 0);
SELECT toTime64('9', 0);
SELECT toTime64('0', 0);

-- Time64 with fractional part, precision 3 (MM:SS.fff)
SELECT toTime64('45:30.123', 3);
SELECT toTime64('00:01.456', 3);
SELECT toTime64('59:59.999', 3);

-- Time64 with fractional part, precision 3 (M:SS.fff)
SELECT toTime64('5:30.123', 3);
SELECT toTime64('9:59.456', 3);
SELECT toTime64('0:01.789', 3);

-- Time64 with fractional part, precision 3 (SS.fff)
SELECT toTime64('45.123', 3);
SELECT toTime64('01.456', 3);
SELECT toTime64('59.999', 3);

-- Time64 with fractional part, precision 3 (S.fff)
SELECT toTime64('5.123', 3);
SELECT toTime64('9.456', 3);
SELECT toTime64('0.789', 3);

-- Negative Time64 with fractional part, precision 3 (MM:SS.fff)
SELECT toTime64('-45:30.123', 3);
SELECT toTime64('-00:01.456', 3);
SELECT toTime64('-59:59.999', 3);

-- Negative Time64 with fractional part, precision 3 (M:SS.fff)
SELECT toTime64('-5:30.123', 3);
SELECT toTime64('-9:59.456', 3);
SELECT toTime64('-0:01.789', 3);

-- Negative Time64 with fractional part, precision 3 (SS.fff)
SELECT toTime64('-45.123', 3);
SELECT toTime64('-01.456', 3);
SELECT toTime64('-59.999', 3);

-- Negative Time64 with fractional part, precision 3 (S.fff)
SELECT toTime64('-5.123', 3);
SELECT toTime64('-9.456', 3);
SELECT toTime64('-0.789', 3);

-- Time64 with fractional part, precision 6 (MM:SS.ffffff)
SELECT toTime64('45:30.123456', 6);
SELECT toTime64('00:01.456789', 6);
SELECT toTime64('59:59.987654', 6);

-- Time64 with fractional part, precision 6 (M:SS.ffffff)
SELECT toTime64('5:30.123456', 6);
SELECT toTime64('9:59.456789', 6);
SELECT toTime64('0:01.987654', 6);

-- Time64 with fractional part, precision 6 (SS.ffffff)
SELECT toTime64('45.123456', 6);
SELECT toTime64('01.456789', 6);
SELECT toTime64('59.987654', 6);

-- Time64 with fractional part, precision 6 (S.ffffff)
SELECT toTime64('5.123456', 6);
SELECT toTime64('9.456789', 6);
SELECT toTime64('0.987654', 6);

-- Time64 with fractional part, precision 9 (MM:SS.fffffffff)
SELECT toTime64('45:30.123456789', 9);
SELECT toTime64('00:01.123456789', 9);
SELECT toTime64('59:59.987654321', 9);

-- Time64 with fractional part, precision 9 (M:SS.fffffffff)
SELECT toTime64('5:30.123456789', 9);
SELECT toTime64('9:59.123456789', 9);
SELECT toTime64('0:01.987654321', 6);

-- Time64 with fractional part, precision 9 (SS.fffffffff)
SELECT toTime64('45.123456789', 9);
SELECT toTime64('01.123456789', 9);
SELECT toTime64('59.987654321', 9);

-- Time64 with fractional part, precision 9 (S.fffffffff)
SELECT toTime64('5.123456789', 9);
SELECT toTime64('9.123456789', 9);
SELECT toTime64('0.987654321', 9);

-- Testing error cases
SELECT toTime('A:30'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT toTime('45:A'); -- { serverError CANNOT_PARSE_TEXT }
SELECT toTime('45:30:15'); -- Not an error, but will be parsed as HH:MM:SS
SELECT toTime64('A:30', 3); -- { serverError CANNOT_PARSE_DATETIME }
SELECT toTime64('45:A.123', 3); -- { serverError CANNOT_PARSE_TEXT }
SELECT toTime64('45:30:15.123', 3); -- Not an error, but will be parsed as HH:MM:SS.fff
