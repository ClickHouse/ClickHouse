set session_timezone = 'Asia/Shanghai';

select 'parseDateTime64InJodaSyntax';
select parseDateTime64InJodaSyntax('', ''); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
select parseDateTime64InJodaSyntax('2077-10-09 10:30:10.123', 'yyyy-MM-dd HH:mm:ss.SSS');
select parseDateTime64InJodaSyntax('2177-10-09 10:30:10.123', 'yyyy-MM-dd HH:mm:ss.SSS'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTime64InJodaSyntax('+0000', 'Z');
select parseDateTime64InJodaSyntax('08:01', 'HH:ss');
select parseDateTime64InJodaSyntax('2024-01-02', 'yyyy-MM-dd');
select parseDateTime64InJodaSyntax('10:30:50', 'HH:mm:ss');
select parseDateTime64InJodaSyntax('2024-12-31 23:30:10.123456-0800', 'yyyy-MM-dd HH:mm:ss.SSSSSSZ');
select parseDateTime64InJodaSyntax('2024-01-01 00:00:01.123456+0800', 'yyyy-MM-dd HH:mm:ss.SSSSSSZ');
select parseDateTime64InJodaSyntax('2024-10-09 10:30:10.123', 'yyyy-MM-dd HH:mm:ss.SSS');
select parseDateTime64InJodaSyntax('2024-10-09 10:30:10.123456', 'yyyy-MM-dd HH:mm:ss.SSSSSS');
select parseDateTime64InJodaSyntax('2024-10-09 10:30:10.123456789', 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTime64InJodaSyntax('2024-10-09 10:30:10.123456-0800', 'yyyy-MM-dd HH:mm:ss.SSSSSSZ');
select parseDateTime64InJodaSyntax('2024-11-05-0800 01:02:03.123456', 'yyyy-MM-ddZ HH:mm:ss.SSSSSS');
select parseDateTime64InJodaSyntax('2024-10-09 10:30:10.123456America/Los_Angeles', 'yyyy-MM-dd HH:mm:ss.SSSSSSz');
select parseDateTime64InJodaSyntax('2024-10-09 10:30:10.123456Australia/Adelaide', 'yyyy-MM-dd HH:mm:ss.SSSSSSz');
select parseDateTime64InJodaSyntax('2024-10-09 10:30:10.123', 'yyyy-dd-MM HH:mm:ss.SSS');
select parseDateTime64InJodaSyntax('999999 10-09-202410:30:10', 'SSSSSSSSS dd-MM-yyyyHH:mm:ss');
select parseDateTime64InJodaSyntax('2024-10-09 10:30:10.123456-0845', 'yyyy-MM-dd HH:mm:ss.SSSSSSZ');
select parseDateTime64InJodaSyntax('2023-02-29 11:22:33Not/Timezone', 'yyyy-MM-dd HH:mm:ssz'); -- { serverError BAD_ARGUMENTS }
--leap years and non-leap years
select parseDateTime64InJodaSyntax('2024-02-29 11:23:34America/Los_Angeles', 'yyyy-MM-dd HH:mm:ssz');
select parseDateTime64InJodaSyntax('2023-02-29 11:22:33America/Los_Angeles', 'yyyy-MM-dd HH:mm:ssz'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTime64InJodaSyntax('2024-02-28 23:22:33America/Los_Angeles', 'yyyy-MM-dd HH:mm:ssz');
select parseDateTime64InJodaSyntax('2023-02-28 23:22:33America/Los_Angeles', 'yyyy-MM-dd HH:mm:ssz');
select parseDateTime64InJodaSyntax('2024-03-01 00:22:33-8000', 'yyyy-MM-dd HH:mm:ssZ');
select parseDateTime64InJodaSyntax('2023-03-01 00:22:33-8000', 'yyyy-MM-dd HH:mm:ssZ');
select 'parseDateTime64InJodaSyntaxOrZero';
select parseDateTime64InJodaSyntaxOrZero('2024-10-09 10:30:10.123', 'yyyy-MM-dd HH:mm:ss.SSS');
select parseDateTime64InJodaSyntaxOrZero('2024-10-09 10:30:10.123456', 'yyyy-MM-dd HH:mm:ss.SSSSSS');
select parseDateTime64InJodaSyntaxOrZero('2024-10-09 10:30:10.123456789', 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS');
select parseDateTime64InJodaSyntaxOrZero('2024-10-09 10:30:10.123456-0800', 'yyyy-MM-dd HH:mm:ss.SSSSSSZ');
select parseDateTime64InJodaSyntaxOrZero('2024-10-09 10:30:10.123456America/Los_Angeles', 'yyyy-MM-dd HH:mm:ss.SSSSSSz');
select parseDateTime64InJodaSyntaxOrZero('2024-10-09 10:30:10.123', 'yyyy-dd-MM HH:mm:ss.SSS');
select parseDateTime64InJodaSyntaxOrZero('wrong value', 'yyyy-dd-MM HH:mm:ss.SSS');
select parseDateTime64InJodaSyntaxOrZero('2023-02-29 11:22:33America/Los_Angeles', 'yyyy-MM-dd HH:mm:ssz');
select parseDateTime64InJodaSyntaxOrZero('', '');
select parseDateTime64InJodaSyntaxOrZero('2177-10-09 10:30:10.123', 'yyyy-MM-dd HH:mm:ss.SSS');
select 'parseDateTime64InJodaSyntaxOrNull';
select parseDateTime64InJodaSyntaxOrNull('2024-10-09 10:30:10.123', 'yyyy-MM-dd HH:mm:ss.SSS');
select parseDateTime64InJodaSyntaxOrNull('2024-10-09 10:30:10.123456', 'yyyy-MM-dd HH:mm:ss.SSSSSS');
select parseDateTime64InJodaSyntaxOrNull('2024-10-09 10:30:10.123456789', 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS');
select parseDateTime64InJodaSyntaxOrNull('2024-10-09 10:30:10.123456-0800', 'yyyy-MM-dd HH:mm:ss.SSSSSSZ');
select parseDateTime64InJodaSyntaxOrNull('2024-10-09 10:30:10.123456America/Los_Angeles', 'yyyy-MM-dd HH:mm:ss.SSSSSSz');
select parseDateTime64InJodaSyntaxOrNull('2024-10-09 10:30:10.123', 'yyyy-dd-MM HH:mm:ss.SSS');
select parseDateTime64InJodaSyntaxOrNull('2023-02-29 11:22:33America/Los_Angeles', 'yyyy-MM-dd HH:mm:ssz');
select parseDateTime64InJodaSyntaxOrNull('', '');
select parseDateTime64InJodaSyntaxOrNull('2177-10-09 10:30:10.123', 'yyyy-MM-dd HH:mm:ss.SSS');

set session_timezone = 'UTC';
select parseDateTime64InJodaSyntax('', '');
