set session_timezone = 'Asia/Shanghai';

select parseDateTime64('2024-10-09 10:30:10.123456'); -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
select parseDateTime64('2024-10-09 10:30:10', 3); -- { serverError BAD_ARGUMENTS }
select parseDateTime64('2024-10-09 10:30:10', -3); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select parseDateTime64('2024-10-09 10:30:10.123456', 6), parseDateTime64('2024-10-09 10:30:10.123456', 6,'%Y-%m-%d %H:%i:%s.%f');
select parseDateTime64('2024-10-09 10:30:10.123456', 6, '%Y-%m-%d %H:%i:%s.%f', 'Etc/GMT-7');
select parseDateTime64('2024-10-09 10:30:10.123', 6, '%Y-%m-%d %H:%i:%s.%f');  -- { serverError NOT_ENOUGH_SPACE }

select parseDateTime64OrZero('2024-10-09 10:30:10.123456'); -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
select parseDateTime64OrZero('2024-10-09 10:30:10', 3); -- { serverError BAD_ARGUMENTS }
select parseDateTime64OrZero('2024-10-09 10:30:10', -3); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select parseDateTime64OrZero('2024-10-09 10:30:10.123456', 6), parseDateTime64OrZero('2024-10-09 10:30:10.123456', 6, '%Y-%m-%d %H:%i:%s.%f');
select parseDateTime64OrZero('2024-10-09 10:30:10.123456', 6, '%Y-%m-%d %H:%i:%s.%f', 'Etc/GMT-7');
select parseDateTime64OrZero('2024-10-09 10:30:10.123456', 6, '%Y-%m-%d %H:%i:%s.%fffff');

select parseDateTime64OrNull('2024-10-09 10:30:10.123456'); -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
select parseDateTime64OrNull('2024-10-09 10:30:10', 3); -- { serverError BAD_ARGUMENTS }
select parseDateTime64OrNull('2024-10-09 10:30:10', -3); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select parseDateTime64OrNull('2024-10-09 10:30:10.123456', 6), parseDateTime64OrZero('2024-10-09 10:30:10.123456', 6,'%Y-%m-%d %H:%i:%s.%f');
select parseDateTime64OrNull('2024-10-09 10:30:10.123456', 6, '%Y-%m-%d %H:%i:%s.%f', 'Etc/GMT-7');;
select parseDateTime64OrZero('2024-10-09 10:30:10.123456', 6, '%Y-%m-%d %H:%i:%s.%fffff');