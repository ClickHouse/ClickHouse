set session_timezone = 'Asia/Shanghai';

select parseDateTime64InJodaSyntax('2024-10-09 10:30:10', 3); -- { serverError NOT_ENOUGH_SPACE }
select parseDateTime64InJodaSyntax('2024-10-09 10:30:10.', 3); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTime64InJodaSyntax('2024-10-09 10:30:10', -3); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select parseDateTime64InJodaSyntax('2024-10-09 10:30:10', 9); -- { serverError BAD_ARGUMENTS }
select parseDateTime64InJodaSyntax('2024-10-09 10:30:10.123', 3), parseDateTime64InJodaSyntax('2024-10-09 10:30:10.123', 6);
select parseDateTime64InJodaSyntax('2024-10-09 10:30:10.123', 'yyyy-MM-dd HH:mm:ss.SSS'), parseDateTime64InJodaSyntax('2024-10-09 10:30:10.123', 'yyyy-MM-dd HH:mm:ss.SSSSSS');
select parseDateTime64InJodaSyntax('2024-10-09 10:30:10.123', 3, 'yyyy-MM-dd HH:mm:ss.SSS'), parseDateTime64InJodaSyntax('2024-10-09 10:30:10.123', 6, 'yyyy-MM-dd HH:mm:ss.SSSSSS');
select parseDateTime64InJodaSyntax('2024-10-09 10:30:10.123', 3, 'yyyy-MM-dd HH:mm:ss.SSSS');  -- { serverError BAD_ARGUMENTS }
select parseDateTime64InJodaSyntax('2024-10-09 10:30:10.123456-0812', 6, 'yyyy-MM-dd HH:mm:ss.SSSSSSZ');
select parseDateTime64InJodaSyntax('2024-10-09 10:30:10.123456-08123', 6, 'yyyy-MM-dd HH:mm:ss.SSSSSSZZZ'); -- {serverError CANNOT_PARSE_DATETIME}
select parseDateTime64InJodaSyntax('2024-10-09 10:30:10.123456America/Los_Angeles', 6, 'yyyy-MM-dd HH:mm:ss.SSSSSSz');
select parseDateTime64InJodaSyntax('2024-10-09 10:30:10.123456America/Los_Angeles', 6, 'yyyy-MM-dd HH:mm:ss.SSSSSSzzz');
-- incorrect timezone offset and timezone
select parseDateTime64InJodaSyntax('2024-10-09 10:30:10.123456-8000', 6, 'yyyy-MM-dd HH:mm:ss.SSSSSSZ'); -- { serverError CANNOT_PARSE_DATETIME }
select parseDateTime64InJodaSyntax('2024-10-09 10:30:10.123456ABCD', 6, 'yyyy-MM-dd HH:mm:ss.SSSSSSz'); -- { serverError BAD_ARGUMENTS }

select parseDateTime64InJodaSyntaxOrZero('2024-10-09 10:30:10', 3);
select parseDateTime64InJodaSyntaxOrZero('2024-10-09 10:30:10.', 3);
select parseDateTime64InJodaSyntaxOrZero('2024-10-09 10:30:10', -3); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select parseDateTime64InJodaSyntaxOrZero('2024-10-09 10:30:10', 9); -- { serverError BAD_ARGUMENTS }
select parseDateTime64InJodaSyntaxOrZero('2024-10-09 10:30:10.123', 3), parseDateTime64InJodaSyntaxOrZero('2024-10-09 10:30:10.123', 6);
select parseDateTime64InJodaSyntaxOrZero('2024-10-09 10:30:10.123', 'yyyy-MM-dd HH:mm:ss.SSS'), parseDateTime64InJodaSyntaxOrZero('2024-10-09 10:30:10.123', 'yyyy-MM-dd HH:mm:ss.SSSSSS');
select parseDateTime64InJodaSyntaxOrZero('2024-10-09 10:30:10.123', 3, 'yyyy-MM-dd HH:mm:ss.SSS'), parseDateTime64InJodaSyntaxOrZero('2024-10-09 10:30:10.123', 6, 'yyyy-MM-dd HH:mm:ss.SSSSSS');
select parseDateTime64InJodaSyntaxOrZero('2024-10-09 10:30:10.123', 3, 'yyyy-MM-dd HH:mm:ss.SSSS');  -- { serverError BAD_ARGUMENTS }
select parseDateTime64InJodaSyntaxOrZero('2024-10-09 10:30:10.123456-0812', 6, 'yyyy-MM-dd HH:mm:ss.SSSSSSZ');
select parseDateTime64InJodaSyntaxOrZero('2024-10-09 10:30:10.123456-08123', 6, 'yyyy-MM-dd HH:mm:ss.SSSSSSZZZ');
select parseDateTime64InJodaSyntaxOrZero('2024-10-09 10:30:10.123456America/Los_Angeles', 6, 'yyyy-MM-dd HH:mm:ss.SSSSSSz');
select parseDateTime64InJodaSyntaxOrZero('2024-10-09 10:30:10.123456America/Los_Angeles', 6, 'yyyy-MM-dd HH:mm:ss.SSSSSSzzz');
-- incorrect timezone offset and timezone
select parseDateTime64InJodaSyntaxOrZero('2024-10-09 10:30:10.123456-8000', 6, 'yyyy-MM-dd HH:mm:ss.SSSSSSZ');
select parseDateTime64InJodaSyntaxOrZero('2024-10-09 10:30:10.123456ABCD', 6, 'yyyy-MM-dd HH:mm:ss.SSSSSSz'); -- { serverError BAD_ARGUMENTS }

select parseDateTime64InJodaSyntaxOrNull('2024-10-09 10:30:10', 3);
select parseDateTime64InJodaSyntaxOrNull('2024-10-09 10:30:10.', 3);
select parseDateTime64InJodaSyntaxOrNull('2024-10-09 10:30:10', -3); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select parseDateTime64InJodaSyntaxOrNull('2024-10-09 10:30:10', 9); -- { serverError BAD_ARGUMENTS }
select parseDateTime64InJodaSyntaxOrNull('2024-10-09 10:30:10.123', 3), parseDateTime64InJodaSyntaxOrNull('2024-10-09 10:30:10.123', 6);
select parseDateTime64InJodaSyntaxOrNull('2024-10-09 10:30:10.123', 'yyyy-MM-dd HH:mm:ss.SSS'), parseDateTime64InJodaSyntaxOrNull('2024-10-09 10:30:10.123', 'yyyy-MM-dd HH:mm:ss.SSSSSS');
select parseDateTime64InJodaSyntaxOrNull('2024-10-09 10:30:10.123', 3, 'yyyy-MM-dd HH:mm:ss.SSS'), parseDateTime64InJodaSyntaxOrNull('2024-10-09 10:30:10.123', 6, 'yyyy-MM-dd HH:mm:ss.SSSSSS');
select parseDateTime64InJodaSyntaxOrNull('2024-10-09 10:30:10.123', 3, 'yyyy-MM-dd HH:mm:ss.SSSS');  -- { serverError BAD_ARGUMENTS }
select parseDateTime64InJodaSyntaxOrNull('2024-10-09 10:30:10.123456-0812', 6, 'yyyy-MM-dd HH:mm:ss.SSSSSSZ');
select parseDateTime64InJodaSyntaxOrNull('2024-10-09 10:30:10.123456-08123', 6, 'yyyy-MM-dd HH:mm:ss.SSSSSSZZZ');
select parseDateTime64InJodaSyntaxOrNull('2024-10-09 10:30:10.123456America/Los_Angeles', 6, 'yyyy-MM-dd HH:mm:ss.SSSSSSz');
select parseDateTime64InJodaSyntaxOrNull('2024-10-09 10:30:10.123456America/Los_Angeles', 6, 'yyyy-MM-dd HH:mm:ss.SSSSSSzzz');
-- incorrect timezone offset and timezone
select parseDateTime64InJodaSyntaxOrNull('2024-10-09 10:30:10.123456-8000', 6, 'yyyy-MM-dd HH:mm:ss.SSSSSSZ');
select parseDateTime64InJodaSyntaxOrNull('2024-10-09 10:30:10.123456ABCD', 6, 'yyyy-MM-dd HH:mm:ss.SSSSSSz'); -- { serverError BAD_ARGUMENTS }