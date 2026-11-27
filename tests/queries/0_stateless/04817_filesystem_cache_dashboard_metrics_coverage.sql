-- Every filesystem cache metric must be plotted on the `Filesystem cache` dashboard.
-- If this test fails, add the metric it prints to the `Filesystem cache` dashboard
-- in `src/Storages/System/StorageSystemDashboardsFilesystemCache.cpp`
-- (or remove the stale reference from it).

SET system_events_show_zero_values = 1;

SELECT 'Filesystem cache metrics missing from the dashboard:';

WITH
    -- Filesystem cache metrics are recognized by their name prefix. The two metrics below
    -- are the only filesystem cache ones which do not follow any of these prefixes, so they
    -- are listed explicitly.
    '^(FileCache|FilesystemCache|FileSegment|CachedReadBuffer|CachedWriteBuffer|CacheWarmer)' AS pattern,
    ['CacheFileSegments', 'CacheDetachedFileSegments'] AS exceptions,
    plotted AS
    (
        SELECT arrayJoin(extractAll(query, '(?:ProfileEvent|CurrentMetric)_\\w+')) AS name
        FROM system.dashboards
        WHERE dashboard = 'Filesystem cache'
    ),
    defined AS
    (
        SELECT 'ProfileEvent_' || event AS name FROM system.events
        WHERE match(event, pattern) OR has(exceptions, event)
        UNION ALL
        SELECT 'CurrentMetric_' || metric AS name FROM system.metrics
        WHERE match(metric, pattern) OR has(exceptions, metric)
    )
SELECT name FROM defined WHERE name NOT IN (SELECT name FROM plotted) ORDER BY name;

WITH plotted AS
    (
        SELECT arrayJoin(extractAll(query, 'metric = \'(\\w+)\'')) AS name
        FROM system.dashboards
        WHERE dashboard = 'Filesystem cache'
    )
SELECT metric FROM system.asynchronous_metrics
WHERE metric LIKE 'FilesystemCache%' AND metric NOT IN (SELECT name FROM plotted)
ORDER BY metric;

SELECT 'Metrics plotted on the dashboard which do not exist:';

WITH
    plotted AS
    (
        SELECT arrayJoin(extractAll(query, '(?:ProfileEvent|CurrentMetric)_\\w+')) AS name
        FROM system.dashboards
        WHERE dashboard = 'Filesystem cache'
    ),
    defined AS
    (
        SELECT 'ProfileEvent_' || event AS name FROM system.events
        UNION ALL
        SELECT 'CurrentMetric_' || metric AS name FROM system.metrics
    )
SELECT DISTINCT name FROM plotted WHERE name NOT IN (SELECT name FROM defined) ORDER BY name;

WITH plotted AS
    (
        SELECT arrayJoin(extractAll(query, 'metric = \'(\\w+)\'')) AS name
        FROM system.dashboards
        WHERE dashboard = 'Filesystem cache'
    )
SELECT DISTINCT name FROM plotted
WHERE name NOT IN (SELECT metric FROM system.asynchronous_metrics)
ORDER BY name;
