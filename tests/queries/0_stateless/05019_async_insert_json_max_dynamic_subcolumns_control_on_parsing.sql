-- `max_dynamic_subcolumns_in_json_type_parsing` must cap dynamic paths the same way for an
-- asynchronous insert as for a synchronous one: the cap must survive the asynchronous insert
-- queue's aggregation into columns built from the table header, not just parsing itself.

DROP TABLE IF EXISTS t_async_json_paths_cap;
CREATE TABLE t_async_json_paths_cap (route String, json JSON) engine=MergeTree PARTITION BY route ORDER BY tuple();

INSERT INTO t_async_json_paths_cap
SETTINGS max_dynamic_subcolumns_in_json_type_parsing = 10, async_insert = 0
VALUES ('sync', '{"a0":0,"a1":1,"a2":2,"a3":3,"a4":4,"a5":5,"a6":6,"a7":7,"a8":8,"a9":9,"a10":10,"a11":11,"a12":12,"a13":13,"a14":14,"a15":15,"a16":16,"a17":17,"a18":18,"a19":19}');

INSERT INTO t_async_json_paths_cap
SETTINGS max_dynamic_subcolumns_in_json_type_parsing = 10, async_insert = 1, wait_for_async_insert = 1
VALUES ('async', '{"a0":0,"a1":1,"a2":2,"a3":3,"a4":4,"a5":5,"a6":6,"a7":7,"a8":8,"a9":9,"a10":10,"a11":11,"a12":12,"a13":13,"a14":14,"a15":15,"a16":16,"a17":17,"a18":18,"a19":19}');

-- `ORDER BY` has to be outside the union: attached to the last branch it sorts only that one.
SELECT * FROM
(
    SELECT route, length(JSONDynamicPaths(json)) AS dynamic_paths, length(JSONSharedDataPaths(json)) AS shared_paths
    FROM t_async_json_paths_cap
    WHERE route = 'sync'
    UNION ALL
    SELECT route, length(JSONDynamicPaths(json)) AS dynamic_paths, length(JSONSharedDataPaths(json)) AS shared_paths
    FROM t_async_json_paths_cap
    WHERE route = 'async'
)
ORDER BY route;

DROP TABLE t_async_json_paths_cap;
