SELECT 'r\\a1bbb' LIKE '%r\\\\a1%bbb%' AS res;

WITH lower('\RealVNC\WinVNC4 /v password') as CommandLine
SELECT
    CommandLine LIKE '%\\\\realvnc\\\\winvnc4%password%' as t1,
    CommandLine LIKE '%\\\\realvnc\\\\winvnc4 %password%' as t2,
    CommandLine LIKE '%\\\\realvnc\\\\winvnc4%password' as t3,
    CommandLine LIKE '%\\\\realvnc\\\\winvnc4 %password' as t4,
    CommandLine LIKE '%realvnc%winvnc4%password%' as t5,
    CommandLine LIKE '%\\\\winvnc4%password%' as t6;
