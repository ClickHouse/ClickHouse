SELECT path('www.example.com:443/a/b/c') AS Path;
SELECT decodeURLComponent(materialize(pathFull('www.example.com/?query=hello%20world+foo%2Bbar'))) AS Path;
