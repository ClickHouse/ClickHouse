SET use_legacy_to_time = 0;

-- Time with three-digit hours
SELECT toTime('000:00:01');
SELECT toTime('001:01:01');
SELECT toTime('100:01:01');
SELECT toTime('999:01:01');
SELECT toTime('999:59:59');
-- Time with two-digit hours
SELECT toTime('00:00:01');
SELECT toTime('01:01:01');
SELECT toTime('10:01:01');
SELECT toTime('99:01:01');
SELECT toTime('99:59:59');
-- Time with one-digit hours
SELECT toTime('0:00:01');
SELECT toTime('1:01:01');
SELECT toTime('0:01:01');
SELECT toTime('9:01:01');
SELECT toTime('9:99:99');
-- Negative Time with three-digit hours
SELECT toTime('-000:00:01');
SELECT toTime('-001:01:01');
SELECT toTime('-100:01:01');
SELECT toTime('-999:01:01');
SELECT toTime('-999:59:59');
-- Negative Time with two-digit hours
SELECT toTime('-00:00:01');
SELECT toTime('-01:01:01');
SELECT toTime('-10:01:01');
SELECT toTime('-99:01:01');
SELECT toTime('-99:99:99');
-- Negative Time with one-digit hours
SELECT toTime('-0:00:01');
SELECT toTime('-1:01:01');
SELECT toTime('-0:01:01');
SELECT toTime('-9:01:01');
SELECT toTime('-9:59:59');

-- Testing Time with minute/second part bigger than 59.
SELECT toTime('-9:99:99');
SELECT toTime('9:99:99');

-- NO FRACTIONAL PART (with trailing last digit)
-- Time64 with three-digit hours
SELECT toTime64('000:00:01', 0);
SELECT toTime64('001:01:01.1', 0);
SELECT toTime64('100:01:01', 0);
SELECT toTime64('999:01:01.1', 0);
SELECT toTime64('999:59:59', 0);
-- Time64 with two-digit hours
SELECT toTime64('00:00:01.1', 0);
SELECT toTime64('01:01:01', 0);
SELECT toTime64('10:01:01.1', 0);
SELECT toTime64('99:01:01', 0);
SELECT toTime64('99:59:59.1', 0);
-- Time64 with one-digit hours
SELECT toTime64('0:00:01', 0);
SELECT toTime64('1:01:01.1', 0);
SELECT toTime64('0:01:01', 0);
SELECT toTime64('9:01:01.1', 0);
SELECT toTime64('9:59:59', 0);
-- Negative Time64 with three-digit hours
SELECT toTime64('-000:00:01.1', 0);
SELECT toTime64('-001:01:01', 0);
SELECT toTime64('-100:01:01.1', 0);
SELECT toTime64('-999:01:01', 0);
SELECT toTime64('-999:59:59.1', 0);
-- Negative Time64 with two-digit hours
SELECT toTime64('-00:00:01', 0);
SELECT toTime64('-01:01:01.1', 0);
SELECT toTime64('-10:01:01', 0);
SELECT toTime64('-99:01:01.1', 0);
SELECT toTime64('-99:59:59', 0);
-- Negative Time64 with one-digit hours
SELECT toTime64('-0:00:01.1', 0);
SELECT toTime64('-1:01:01', 0);
SELECT toTime64('-0:01:01.1', 0);
SELECT toTime64('-9:01:01', 0);
SELECT toTime64('-9:59:59.1', 0);

-- FRACTIONAL PART WITH SIZE 3
-- Time64 with three-digit hours
SELECT toTime64('000:00:01.123', 3);
SELECT toTime64('001:01:01.1234', 3);
SELECT toTime64('100:01:01.123', 3);
SELECT toTime64('999:01:01.1234', 3);
SELECT toTime64('999:59:59.123', 3);
-- Time64 with two-digit hours
SELECT toTime64('00:00:01.123', 3);
SELECT toTime64('01:01:01.1234', 3);
SELECT toTime64('10:01:01.123', 3);
SELECT toTime64('99:01:01.1234', 3);
SELECT toTime64('99:59:59.123', 3);
-- Time64 with one-digit hours
SELECT toTime64('0:00:01.123', 3);
SELECT toTime64('1:01:01.1234', 3);
SELECT toTime64('0:01:01.123', 3);
SELECT toTime64('9:01:01.1234', 3);
SELECT toTime64('9:59:59.123', 3);
-- Negative Time64 with three-digit hours
SELECT toTime64('-000:00:01.123', 3);
SELECT toTime64('-001:01:01.1234', 3);
SELECT toTime64('-100:01:01.123', 3);
SELECT toTime64('-999:01:01.1234', 3);
SELECT toTime64('-999:59:59.123', 3);
-- Negative Time64 with two-digit hours
SELECT toTime64('-00:00:01.123', 3);
SELECT toTime64('-01:01:01.1234', 3);
SELECT toTime64('-10:01:01.123', 3);
SELECT toTime64('-99:01:01.1234', 3);
SELECT toTime64('-99:59:59.123', 3);
-- Negative Time64 with one-digit hours
SELECT toTime64('-0:00:01.123', 3);
SELECT toTime64('-1:01:01.1234', 3);
SELECT toTime64('-0:01:01.123', 3);
SELECT toTime64('-9:01:01.1234', 3);
SELECT toTime64('-9:59:59.123', 3);

-- FRACTIONAL PART WITH SIZE 6
-- Time64 with three-digit hours
SELECT toTime64('000:00:01.123456', 6);
SELECT toTime64('001:01:01.1234567', 6);
SELECT toTime64('100:01:01.123456', 6);
SELECT toTime64('999:01:01.1234567', 6);
SELECT toTime64('999:59:59.123456', 6);
-- Time64 with two-digit hours
SELECT toTime64('00:00:01.1234567', 6);
SELECT toTime64('01:01:01.123456', 6);
SELECT toTime64('10:01:01.1234567', 6);
SELECT toTime64('99:01:01.123456', 6);
SELECT toTime64('99:59:59.1234567', 6);
-- Time64 with one-digit hours
SELECT toTime64('0:00:01.123456', 6);
SELECT toTime64('1:01:01.1234567', 6);
SELECT toTime64('0:01:01.123456', 6);
SELECT toTime64('9:01:01.1234567', 6);
SELECT toTime64('9:59:59.123456', 6);
-- Negative Time64 with three-digit hours
SELECT toTime64('-000:00:01.1234567', 6);
SELECT toTime64('-001:01:01.123456', 6);
SELECT toTime64('-100:01:01.1234567', 6);
SELECT toTime64('-999:01:01.123456', 6);
SELECT toTime64('-999:59:59.1234567', 6);
-- Negative Time64 with two-digit hours
SELECT toTime64('-00:00:01.123456', 6);
SELECT toTime64('-01:01:01.1234567', 6);
SELECT toTime64('-10:01:01.123456', 6);
SELECT toTime64('-99:01:01.1234567', 6);
SELECT toTime64('-99:59:59.123456', 6);
-- Negative Time64 with one-digit hours
SELECT toTime64('-0:00:01.1234567', 6);
SELECT toTime64('-1:01:01.123456', 6);
SELECT toTime64('-0:01:01.1234567', 6);
SELECT toTime64('-9:01:01.123456', 6);
SELECT toTime64('-9:59:59.1234567', 6);

-- FRACTIONAL PART WITH SIZE 7
-- Time64 with three-digit hours
SELECT toTime64('000:00:01.1234567', 7);
SELECT toTime64('001:01:01.12345678', 7);
SELECT toTime64('100:01:01.1234567', 7);
SELECT toTime64('999:01:01.12345678', 7);
SELECT toTime64('999:59:59.1234567', 7);
-- Time64 with two-digit hours
SELECT toTime64('00:00:01.12345678', 7);
SELECT toTime64('01:01:01.1234567', 7);
SELECT toTime64('10:01:01.12345678', 7);
SELECT toTime64('99:01:01.1234567', 7);
SELECT toTime64('99:59:59.12345678', 7);
-- Time64 with one-digit hours
SELECT toTime64('0:00:01.1234567', 7);
SELECT toTime64('1:01:01.12345678', 7);
SELECT toTime64('0:01:01.1234567', 7);
SELECT toTime64('9:01:01.12345678', 7);
SELECT toTime64('9:59:59.1234567', 7);
-- Negative Time64 with three-digit hours
SELECT toTime64('-000:00:01.12345678', 7);
SELECT toTime64('-001:01:01.1234567', 7);
SELECT toTime64('-100:01:01.12345678', 7);
SELECT toTime64('-999:01:01.1234567', 7);
SELECT toTime64('-999:59:59.12345678', 7);
-- Negative Time64 with two-digit hours
SELECT toTime64('-00:00:01.1234567', 7);
SELECT toTime64('-01:01:01.12345678', 7);
SELECT toTime64('-10:01:01.1234567', 7);
SELECT toTime64('-99:01:01.12345678', 7);
SELECT toTime64('-99:59:59.1234567', 7);
-- Negative Time64 with one-digit hours
SELECT toTime64('-0:00:01.12345678', 7);
SELECT toTime64('-1:01:01.1234567', 7);
SELECT toTime64('-0:01:01.12345678', 7);
SELECT toTime64('-9:01:01.1234567', 7);
SELECT toTime64('-9:59:59.12345678', 7);

-- FRACTIONAL PART WITH SIZE 9
-- Time64 with three-digit hours
SELECT toTime64('000:00:01.1234567891', 9);
SELECT toTime64('001:01:01.123456789', 9);
SELECT toTime64('100:01:01.1234567891', 9);
SELECT toTime64('999:01:01.123456789', 9);
SELECT toTime64('999:59:59.1234567891', 9);
-- Time64 with two-digit hours
SELECT toTime64('00:00:01.123456789', 9);
SELECT toTime64('01:01:01.1234567891', 9);
SELECT toTime64('10:01:01.123456789', 9);
SELECT toTime64('99:01:01.1234567891', 9);
SELECT toTime64('99:59:59.123456789', 9);
-- Time64 with one-digit hours
SELECT toTime64('0:00:01.1234567891', 9);
SELECT toTime64('1:01:01.123456789', 9);
SELECT toTime64('0:01:01.1234567891', 9);
SELECT toTime64('9:01:01.123456789', 9);
SELECT toTime64('9:59:59.1234567891', 9);
-- Negative Time64 with three-digit hours
SELECT toTime64('-000:00:01.123456789', 9);
SELECT toTime64('-001:01:01.1234567891', 9);
SELECT toTime64('-100:01:01.123456789', 9);
SELECT toTime64('-999:01:01.1234567891', 9);
SELECT toTime64('-999:59:59.123456789', 9);
-- Negative Time64 with two-digit hours
SELECT toTime64('-00:00:01.1234567891', 9);
SELECT toTime64('-01:01:01.123456789', 9);
SELECT toTime64('-10:01:01.1234567891', 9);
SELECT toTime64('-99:01:01.123456789', 9);
SELECT toTime64('-99:59:59.1234567891', 9);
-- Negative Time64 with one-digit hours
SELECT toTime64('-0:00:01.123456789', 9);
SELECT toTime64('-1:01:01.1234567891', 9);
SELECT toTime64('-0:01:01.123456789', 9);
SELECT toTime64('-9:01:01.1234567891', 9);
SELECT toTime64('-9:59:59.123456789', 9);

-- Testing Time64 with minute/second part bigger than 59.
SELECT toTime64('-9:99:99', 0);
SELECT toTime64('9:99:99', 0);
SELECT toTime64('-9:99:99.1234', 3);
SELECT toTime64('9:99:99.1234', 3);

-- Testing cases where error excected
SELECT toTime('a'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT toTime64('a', 0); -- { serverError CANNOT_PARSE_DATETIME }
