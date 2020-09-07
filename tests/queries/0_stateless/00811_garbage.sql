SET send_logs_level = 'none';

SELECT truncate(895, -16);
SELECT ( SELECT toDecimal128([], rowNumberInBlock()) ) , lcm('', [[(CAST(('>A') AS String))]]); -- { serverError 44 }
