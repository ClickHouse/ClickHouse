set session_timezone='UTC';

select parseDateTime64BestEffort('1744042005.1', 1);
select parseDateTime64BestEffort('1744042005.12', 2);
select parseDateTime64BestEffort('1744042005.123', 3);
select parseDateTime64BestEffort('1744042005.1234', 4);
select parseDateTime64BestEffort('1744042005.12345', 5);
select parseDateTime64BestEffort('1744042005.123456', 6);
select parseDateTime64BestEffort('1744042005.1234567', 7);
select parseDateTime64BestEffort('1744042005.12345678', 8);
select parseDateTime64BestEffort('1744042005.123456789', 9);

select parseDateTime64BestEffort('174404200.1', 1);
select parseDateTime64BestEffort('174404200.12', 2);
select parseDateTime64BestEffort('174404200.123', 3);
select parseDateTime64BestEffort('174404200.1234', 4);
select parseDateTime64BestEffort('174404200.12345', 5);
select parseDateTime64BestEffort('174404200.123456', 6);
select parseDateTime64BestEffort('174404200.1234567', 7);
select parseDateTime64BestEffort('174404200.12345678', 8);
select parseDateTime64BestEffort('174404200.123456789', 9);

