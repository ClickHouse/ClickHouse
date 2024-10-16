select parseDateTime64InJodaSyntaxOrNull('2024-10-09 10:30:10.123', 'yyyy-MM-dd HH:mm:ss.SSS');
select parseDateTime64InJodaSyntaxOrNull('2024-10-09 10:30:10.123456', 'yyyy-MM-dd HH:mm:ss.SSSSSS');
select parseDateTime64InJodaSyntaxOrNull('2024-10-09 10:30:10.123456789', 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS');
select parseDateTime64InJodaSyntaxOrNull('2024-10-09 10:30:10.123456-0800', 'yyyy-MM-dd HH:mm:ss.SSSSSSZ');
select parseDateTime64InJodaSyntaxOrNull('2024-10-09 10:30:10.123456Asia/Shanghai', 'yyyy-MM-dd HH:mm:ss.SSSSSSz');