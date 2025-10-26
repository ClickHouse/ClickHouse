
with toDate('2023-01-09') as date_mon, date_mon - 1 as date_sun select toDayOfWeek(date_mon), toDayOfWeek(date_sun);
with toDate('2023-01-09') as date_mon, date_mon - 1 as date_sun select toDayOfWeek(date_mon, 0), toDayOfWeek(date_sun, 0);
with toDate('2023-01-09') as date_mon, date_mon - 1 as date_sun select toDayOfWeek(date_mon, 1), toDayOfWeek(date_sun, 1);
with toDate('2023-01-09') as date_mon, date_mon - 1 as date_sun select toDayOfWeek(date_mon, 2), toDayOfWeek(date_sun, 2);
with toDate('2023-01-09') as date_mon, date_mon - 1 as date_sun select toDayOfWeek(date_mon, 3), toDayOfWeek(date_sun, 3);
with toDate('2023-01-09') as date_mon, date_mon - 1 as date_sun select toDayOfWeek(date_mon, 4), toDayOfWeek(date_sun, 4);
with toDate('2023-01-09') as date_mon, date_mon - 1 as date_sun select toDayOfWeek(date_mon, 5), toDayOfWeek(date_sun, 5);

select toDayOfWeek(today(), -1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
