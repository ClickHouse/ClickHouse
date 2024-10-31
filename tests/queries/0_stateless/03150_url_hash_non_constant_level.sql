WITH 'https://www3.botinok.co.edu.il/~kozlevich/CGI-BIN/WEBSIT~0.DLL?longptr=0xFFFFFFFF&ONERR=CONTINUE#!PGNUM=99' AS url SELECT URLHash(url, arrayJoin(range(10)));
SELECT '---';
WITH 'https://www3.botinok.co.edu.il/~kozlevich/CGI-BIN/WEBSIT~0.DLL?longptr=0xFFFFFFFF&ONERR=CONTINUE#!PGNUM=99' AS url SELECT URLHash(materialize(url), arrayJoin(range(10)));
SELECT '---';
WITH 'https://www3.botinok.co.edu.il/~kozlevich/CGI-BIN/WEBSIT~0.DLL?longptr=0xFFFFFFFF&ONERR=CONTINUE#!PGNUM=99' AS url SELECT cityHash64(substring(x, -1, 1) IN ('/', '?', '#') ? substring(x, 1, -1) : x), arrayJoin(URLHierarchy(url)) AS x;
SELECT '---';
WITH 'https://www3.botinok.co.edu.il/~kozlevich/CGI-BIN/WEBSIT~0.DLL?longptr=0xFFFFFFFF&ONERR=CONTINUE#!PGNUM=99' AS url SELECT cityHash64(substring(x, -1, 1) IN ('/', '?', '#') ? substring(x, 1, -1) : x), arrayJoin(URLHierarchy(materialize(url))) AS x;
