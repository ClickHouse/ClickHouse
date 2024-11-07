set session_timezone = 'Asia/Shanghai';

select parseDateTime64('2024-10-09 10:30:10.123456');
select parseDateTime64('2024-10-09 10:30:10.123'); -- { serverError NOT_ENOUGH_SPACE }
select parseDateTime64('2024-10-09 10:30:10', 3); -- { serverError NOT_ENOUGH_SPACE }
select parseDateTime64('2024-10-09 10:30:10.', 3); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTime64('2024-10-09 10:30:10', -3); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select parseDateTime64('2024-10-09 10:30:10', 9); -- { serverError BAD_ARGUMENTS }
select parseDateTime64('2024-10-09 10:30:10.123456', 6), parseDateTime64('2024-10-09 10:30:10.123456', '%Y-%m-%d %H:%i:%s.%f');
select parseDateTime64('2024-10-09 10:30:10.123456', 6, '%Y-%m-%d %H:%i:%s.%f'), parseDateTime64('2024-10-09 10:30:10.123456', 6, '%Y-%m-%d %H:%i:%s.%f', 'Etc/GMT-7');
select parseDateTime64('2024-10-09 10:30:10.123', 3, '%Y-%m-%d %H:%i:%s.%f');  -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTime64('2024-10-09 10:30:10.123', 6, '%Y-%m-%d %H:%i:%s.%f');  -- { serverError NOT_ENOUGH_SPACE }

select parseDateTime64OrZero('2024-10-09 10:30:10.123456');
select parseDateTime64OrZero('2024-10-09 10:30:10.123');
select parseDateTime64OrZero('2024-10-09 10:30:10', 3);
select parseDateTime64OrZero('2024-10-09 10:30:10.', 3);
select parseDateTime64OrZero('2024-10-09 10:30:10', -3); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select parseDateTime64OrZero('2024-10-09 10:30:10', 9); -- { serverError BAD_ARGUMENTS }
select parseDateTime64OrZero('2024-10-09 10:30:10.123456', 6), parseDateTime64OrZero('2024-10-09 10:30:10.123456', '%Y-%m-%d %H:%i:%s.%f');
select parseDateTime64OrZero('2024-10-09 10:30:10.123456', 6, '%Y-%m-%d %H:%i:%s.%f'), parseDateTime64OrZero('2024-10-09 10:30:10.123456', 6, '%Y-%m-%d %H:%i:%s.%f', 'Etc/GMT-7');
select parseDateTime64OrZero('2024-10-09 10:30:10.123', 3, '%Y-%m-%d %H:%i:%s.%f');

select parseDateTime64OrNull('2024-10-09 10:30:10.123456');
select parseDateTime64OrNull('2024-10-09 10:30:10.123');
select parseDateTime64OrNull('2024-10-09 10:30:10', 3);
select parseDateTime64OrNull('2024-10-09 10:30:10.', 3);
select parseDateTime64OrNull('2024-10-09 10:30:10', -3); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select parseDateTime64OrNull('2024-10-09 10:30:10', 9); -- { serverError BAD_ARGUMENTS }
select parseDateTime64OrNull('2024-10-09 10:30:10.123456', 6), parseDateTime64OrZero('2024-10-09 10:30:10.123456', '%Y-%m-%d %H:%i:%s.%f');
select parseDateTime64OrNull('2024-10-09 10:30:10.123456', 6, '%Y-%m-%d %H:%i:%s.%f'), parseDateTime64OrNull('2024-10-09 10:30:10.123456', 6, '%Y-%m-%d %H:%i:%s.%f', 'Etc/GMT-7');;
select parseDateTime64OrNull('2024-10-09 10:30:10.123', 3, '%Y-%m-%d %H:%i:%s.%f');