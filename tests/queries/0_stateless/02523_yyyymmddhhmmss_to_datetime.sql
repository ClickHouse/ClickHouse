select YYYYMMDDhhmmssToDateTime(19910824210400);
select YYYYMMDDhhmmssToDateTime(19910824210400, 'CET');
select cast(YYYYMMDDhhmmssToDateTime(19910824210400, 'CET') as DateTime('UTC'));

select toTypeName(YYYYMMDDhhmmssToDateTime(19910824210400));
select toTypeName(YYYYMMDDhhmmssToDateTime(19910824210400, 'CET'));

select YYYYMMDDhhmmssToDateTime(19250101000000, 'UTC');
select YYYYMMDDhhmmssToDateTime(19241231235959, 'UTC');
select YYYYMMDDhhmmssToDateTime(22831111235959, 'UTC');
select YYYYMMDDhhmmssToDateTime(22831112000000, 'UTC');
select YYYYMMDDhhmmssToDateTime(22620411234716, 'UTC');
select YYYYMMDDhhmmssToDateTime(22620411234717, 'UTC');

select YYYYMMDDhhmmssToDateTime(19840001000000, 'UTC');
select YYYYMMDDhhmmssToDateTime(19840100000000, 'UTC');
select YYYYMMDDhhmmssToDateTime(19841301000000, 'UTC');
select YYYYMMDDhhmmssToDateTime(19840141000000, 'UTC');
select YYYYMMDDhhmmssToDateTime(19840101250000, 'UTC');
select YYYYMMDDhhmmssToDateTime(19840101007000, 'UTC');
select YYYYMMDDhhmmssToDateTime(19840101000070, 'UTC');
select YYYYMMDDhhmmssToDateTime(19840101000000, 'not a timezone'); -- { serverError 1000 }

select YYYYMMDDhhmmssToDateTime(19840101000000, 'UTC');
select YYYYMMDDhhmmssToDateTime(19830229000000, 'UTC');
select YYYYMMDDhhmmssToDateTime(-19840101000000, 'UTC');

select YYYYMMDDhhmmssToDateTime(655370824210400, 'UTC');