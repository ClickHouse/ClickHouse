select YYYYMMDDhhmmssToDateTime64(19910824210400);
select YYYYMMDDhhmmssToDateTime64(19910824210400.123);
select YYYYMMDDhhmmssToDateTime64(19910824210400.1234, 6);
select YYYYMMDDhhmmssToDateTime64(19910824210400.1234, 7, 'CET');
select cast(YYYYMMDDhhmmssToDateTime64(19910824210400.1234, 7, 'CET') as DateTime64(7, 'UTC'));

select toTypeName(YYYYMMDDhhmmssToDateTime64(19910824210400));
select toTypeName(YYYYMMDDhhmmssToDateTime64(19910824210400.123));
select toTypeName(YYYYMMDDhhmmssToDateTime64(19910824210400.1234, 6));
select toTypeName(YYYYMMDDhhmmssToDateTime64(19910824210400.1234, 7, 'CET'));
select toTypeName(cast(YYYYMMDDhhmmssToDateTime64(19910824210400.1234, 7, 'CET') as DateTime64(7, 'UTC')));

select YYYYMMDDhhmmssToDateTime64(19000101000000, 9, 'UTC');
select YYYYMMDDhhmmssToDateTime64(18991231235959.999999999, 9, 'UTC');
select YYYYMMDDhhmmssToDateTime64(22991231235959.99999999, 8, 'UTC');
select YYYYMMDDhhmmssToDateTime64(22991231235959.999999999, 9, 'UTC'); -- { serverError 407 }
select YYYYMMDDhhmmssToDateTime64(22620411234716.1111111, 9, 'UTC');
select YYYYMMDDhhmmssToDateTime64(22620411234716.854775808, 9, 'UTC'); -- { serverError 407 }
select YYYYMMDDhhmmssToDateTime64(22620411234716.85477581, 8, 'UTC');

select YYYYMMDDhhmmssToDateTime64(19910824210400.1234, 0, 'CET');
select YYYYMMDDhhmmssToDateTime64(19910824210400.1234, 1, 'CET');
select YYYYMMDDhhmmssToDateTime64(19910824210400.1234, 2, 'CET');
select YYYYMMDDhhmmssToDateTime64(19910824210400.1234, 3, 'CET');
select YYYYMMDDhhmmssToDateTime64(19910824210400.1234, 4, 'CET');
select YYYYMMDDhhmmssToDateTime64(19910824210400.1234, 5, 'CET');
select YYYYMMDDhhmmssToDateTime64(19910824210400.1234, 6, 'CET');
select YYYYMMDDhhmmssToDateTime64(19910824210400.1234, 7, 'CET');
select YYYYMMDDhhmmssToDateTime64(19910824210400.1234, 8, 'CET');
select YYYYMMDDhhmmssToDateTime64(19910824210400.1234, 9, 'CET');
select YYYYMMDDhhmmssToDateTime64(19910824210400.1234, 10, 'CET'); -- { serverError 69 }
select YYYYMMDDhhmmssToDateTime64(19910824210400.1234, -1, 'CET'); -- { serverError 69 }

select YYYYMMDDhhmmssToDateTime64(19840001000000, 9, 'UTC');
select YYYYMMDDhhmmssToDateTime64(19840100000000, 9, 'UTC');
select YYYYMMDDhhmmssToDateTime64(19841301000000, 9, 'UTC');
select YYYYMMDDhhmmssToDateTime64(19840141000000, 9, 'UTC');
select YYYYMMDDhhmmssToDateTime64(19840101250000, 9, 'UTC');
select YYYYMMDDhhmmssToDateTime64(19840101007000, 9, 'UTC');
select YYYYMMDDhhmmssToDateTime64(19840101000070, 9, 'UTC');
select YYYYMMDDhhmmssToDateTime64(19840101000000, 9, 'not a timezone'); -- { serverError 1000 }

select YYYYMMDDhhmmssToDateTime64(19840101020304.5, 9, 'UTC');
select YYYYMMDDhhmmssToDateTime64(19840229020304.5, 9, 'UTC');
select YYYYMMDDhhmmssToDateTime64(19830229020304.5, 9, 'UTC');
select YYYYMMDDhhmmssToDateTime64(19840230020304.5, 9, 'UTC');
select YYYYMMDDhhmmssToDateTime64(19830230020304.5, 9, 'UTC');
select YYYYMMDDhhmmssToDateTime64(19840231020304.5, 9, 'UTC');
select YYYYMMDDhhmmssToDateTime64(19830231020304.5, 9, 'UTC');
select YYYYMMDDhhmmssToDateTime64(19840232020304.5, 9, 'UTC');
select YYYYMMDDhhmmssToDateTime64(19830232020304.5, 9, 'UTC');

select YYYYMMDDhhmmssToDateTime64(-19840101020304.5, 9, 'UTC');

select YYYYMMDDhhmmssToDateTime64(NAN, 9, 'UTC');

select YYYYMMDDhhmmssToDateTime64(655370824210400);
