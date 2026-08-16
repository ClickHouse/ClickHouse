-- Query-level SETTINGS on a UNION scope query parameters across every branch.
SELECT {x:UInt64} UNION ALL SELECT 0 SETTINGS param_x = '1';

-- Query-level SETTINGS after FORMAT are carried by ASTQueryWithOutput.
SELECT 1 FORMAT Null SETTINGS max_threads = {t:UInt64}, param_t = '1';
