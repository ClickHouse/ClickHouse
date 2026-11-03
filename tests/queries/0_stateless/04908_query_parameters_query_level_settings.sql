-- Query-level SETTINGS on a UNION scope query parameters across every branch.
SELECT {x:UInt64} UNION ALL SELECT 0 FORMAT Null SETTINGS param_x = '1';
