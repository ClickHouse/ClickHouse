-- Tags: no-old-analyzer

-- `planToASCII` renders the `query_plan` JSON of `system.query_log` as the text tree of
-- `EXPLAIN ANALYZE`. The plans below are written by hand, so the output does not depend on timings.

-- Every kind of line: details, a detail that a statistics group replaces (`Cost:` and `Left:`),
-- indexes, projections, metric groups whose keys are out of order (a `JSON` column sorts them), metrics that
-- were not collected (null), a group this function does not know, the time and concurrency
-- groups, labelled and unlabelled stages, inline stage metrics, and a stage that did no work.
SELECT planToASCII('{
    "Version": 1,
    "Root": "Expression_3",
    "ExecutionTimeNs": 2000000,
    "MaxThreads": 4,
    "Output": ["k", "c"],
    "Nodes": [
        {"NodeType": "Expression", "NodeId": "Expression_3", "Description": "Project names", "Details": [], "Children": ["Join_2"],
         "Statistics": {
            "IO": {"InputRows": 10, "OutputRows": 10, "InputBytes": 0, "OutputBytes": 0},
            "Time": {"StepTimeNs": 1000, "StepTimeShare": 0.05, "BranchTimeNs": 1500000, "BranchTimeShare": 75},
            "Concurrency": {"Step": {"Numerator": 1, "Denominator": 4}, "Branch": null},
            "Stages": [{"Name": "", "GroupId": 0, "WallClockTimeNs": 1200, "Processors": 1, "ProcessorTimeNs": {"Min": 1000, "Median": 1000, "Max": 1000, "Sum": 1000}}]}},
        {"NodeType": "Join", "NodeId": "Join_2", "Description": "JOIN FillRightFirst",
         "Details": ["t1 ⋈ t2", "Type: inner", "Cost: estimated 100", "Left: rows estimated 50", "Join conditions: k = k"],
         "Children": ["ReadFromMergeTree_0", "ReadFromSystemNumbers_1"],
         "Statistics": {
            "Right": {"Rows": 40, "RowsEstimated": null, "Matched": null},
            "Left": {"Fanout": 1, "Rows": 5000, "RowsEstimated": 50, "MatchRate": 100, "Matched": 5000},
            "Cost": {"Estimated": 100, "Actual": 5000},
            "HashTable": {"UniqueKeys": 40, "Memory": 4096},
            "FutureGroup": {"NewMetric": 7},
            "IO": {"InputRows": 5040, "OutputRows": 10, "InputBytes": 80640, "OutputBytes": 160},
            "Stages": [
                {"Name": "build", "GroupId": 1, "WallClockTimeNs": 300000, "Processors": 1, "ProcessorTimeNs": {"Sum": 290000}},
                {"Name": "probe", "GroupId": 2, "WallClockTimeNs": 500000, "Processors": 8, "ProcessorTimeNs": {"Sum": 1500000},
                 "Metrics": {"SortShare": 12.5, "SortTime": 62500}}]}},
        {"NodeType": "ReadFromMergeTree", "NodeId": "ReadFromMergeTree_0", "Description": "default.t", "Details": ["Read type: Default"], "Children": [],
         "Indexes": [
            {"Type": "MinMax", "Keys": ["d"], "Condition": "true", "Selected Parts": 2, "Selected Granules": 10},
            {"Type": "PrimaryKey", "Keys": ["k", "d"], "Condition": "(k in [1, +Inf))", "Initial Parts": 2, "Selected Parts": 1,
             "Initial Granules": 10, "Selected Granules": 3, "Search Algorithm": "binary search"}],
         "Projections": [
            {"Selected Rows": 8192, "Selected Ranges": 1, "Selected Parts": 1, "Selected Marks": 1, "Name": "p_by_d",
             "Filtered Parts": 1, "Description": "Projection has been analyzed and is used for part-level filtering",
             "Condition": "(d in [1, 1])", "Search Algorithm": "binary search"}],
         "Statistics": {"IO": {"InputRows": 0, "OutputRows": 5000, "InputBytes": 0, "OutputBytes": 80000}, "Stages": []}},
        {"NodeType": "ReadFromSystemNumbers", "NodeId": "ReadFromSystemNumbers_1", "Details": [], "Children": [],
         "Statistics": {"IO": {"InputRows": 0, "OutputRows": 40, "InputBytes": 0, "OutputBytes": 640},
            "Stages": [{"Name": "", "GroupId": 0, "WallClockTimeNs": 0, "Processors": 1}]}}
    ]
}') FORMAT TSVRaw;

-- Runtime filters: the `RuntimeFilter` group is printed on one line, as `EXPLAIN ANALYZE` does,
-- with the reason of a disabled filter after its type and the unique keys not rounded.
SELECT planToASCII('{
    "Version": 1,
    "Root": "BuildRuntimeFilter_1",
    "Output": ["k"],
    "Nodes": [
        {"NodeType": "BuildRuntimeFilter", "NodeId": "BuildRuntimeFilter_1", "Details": [], "Children": ["BuildRuntimeFilter_0"],
         "Statistics": {
            "RuntimeFilter": {"UniqueKeys": 123456, "Type": "exact set", "Size": 32768, "Replaced": "bloom filter"},
            "Probe": {"RowsSkipped": 0, "RowsPassed": 50, "RowsChecked": 200, "PassRate": 25, "RowsBeforeReady": 0},
            "IO": {"InputRows": 100, "OutputRows": 100, "InputBytes": 0, "OutputBytes": 0},
            "Stages": []}},
        {"NodeType": "BuildRuntimeFilter", "NodeId": "BuildRuntimeFilter_0", "Details": [], "Children": [],
         "Statistics": {
            "RuntimeFilter": {"SetBits": 0.07, "Size": 524288, "Reason": "bloom filter saturated", "Type": "disabled"},
            "Probe": {"RowsSkipped": 1000, "RowsPassed": 0, "RowsChecked": 0, "RowsBeforeReady": 10},
            "IO": {"InputRows": 100, "OutputRows": 100, "InputBytes": 0, "OutputBytes": 0},
            "Stages": []}}
    ]
}') FORMAT TSVRaw;

-- The plan of a query that threw has no statistics: only the tree is printed.
SELECT planToASCII('{"Version": 1, "Root": "Expression_1", "Output": ["x"], "Nodes": [
    {"NodeType": "Expression", "NodeId": "Expression_1", "Details": [], "Children": ["ReadFromSystemOne_0"]},
    {"NodeType": "ReadFromSystemOne", "NodeId": "ReadFromSystemOne_0", "Details": [], "Children": []}]}') FORMAT TSVRaw;

-- The argument can be the `JSON` column itself, not only its text.
SELECT planToASCII('{"Version": 1, "Root": "ReadFromSystemOne_0", "Output": ["dummy"], "Nodes": [
    {"NodeType": "ReadFromSystemOne", "NodeId": "ReadFromSystemOne_0", "Details": [], "Children": [],
     "Statistics": {"IO": {"InputRows": 0, "OutputRows": 1, "InputBytes": 0, "OutputBytes": 1}, "Stages": []}}]}'::JSON) FORMAT TSVRaw;

-- No plan: `NULL`, an empty string, and the default of the column, an empty object.
SELECT planToASCII(NULL), planToASCII(''), planToASCII('{}'), planToASCII('{}'::JSON);

-- A plan that could not be logged is replaced by its reason.
SELECT planToASCII('{"Error": "The query plan is too large"}') FORMAT TSVRaw;

SELECT planToASCII('not json'); -- { serverError BAD_ARGUMENTS }
SELECT planToASCII('[1, 2]'); -- { serverError BAD_ARGUMENTS }
SELECT planToASCII('{"Root": "A", "Nodes": []}'); -- { serverError BAD_ARGUMENTS }
SELECT planToASCII('{"Version": 99, "Root": "A", "Nodes": []}'); -- { serverError BAD_ARGUMENTS }
SELECT planToASCII('{"Version": 1, "Nodes": []}'); -- { serverError BAD_ARGUMENTS }
SELECT planToASCII('{"Version": 1, "Root": "A"}'); -- { serverError BAD_ARGUMENTS }
SELECT planToASCII('{"Version": 1, "Root": "A", "Nodes": [{"NodeType": "T", "NodeId": "A", "Children": ["B"]}]}'); -- { serverError BAD_ARGUMENTS }
SELECT planToASCII('{"Version": 1, "Root": "A", "Nodes": [{"NodeType": "T", "NodeId": "A", "Children": ["A"]}]}'); -- { serverError BAD_ARGUMENTS }
SELECT planToASCII(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- A count that is not a non-negative integer fitting `UInt64`: negative, too large, 2^64 as a
-- double (which rounds to the same double as the maximum of `UInt64`), fractional, or not a number.
SELECT planToASCII('{"Version": 1, "MaxThreads": -1, "Root": "A", "Nodes": [{"NodeType": "T", "NodeId": "A"}]}'); -- { serverError BAD_ARGUMENTS }
SELECT planToASCII('{"Version": 1, "MaxThreads": 1e30, "Root": "A", "Nodes": [{"NodeType": "T", "NodeId": "A"}]}'); -- { serverError BAD_ARGUMENTS }
SELECT planToASCII('{"Version": 1, "MaxThreads": 1.5, "Root": "A", "Nodes": [{"NodeType": "T", "NodeId": "A"}]}'); -- { serverError BAD_ARGUMENTS }
SELECT planToASCII('{"Version": 1, "MaxThreads": "-1", "Root": "A", "Nodes": [{"NodeType": "T", "NodeId": "A"}]}'); -- { serverError BAD_ARGUMENTS }
SELECT planToASCII('{"Version": 18446744073709551616.0, "Root": "A", "Nodes": []}'); -- { serverError BAD_ARGUMENTS }
SELECT planToASCII('{"Version": "18446744073709551616", "Root": "A", "Nodes": []}'); -- { serverError BAD_ARGUMENTS }
SELECT planToASCII('{"Version": "one", "Root": "A", "Nodes": []}'); -- { serverError BAD_ARGUMENTS }
SELECT planToASCII('{"Version": 1, "Root": "A", "Nodes": [{"NodeType": "T", "NodeId": "A", "Statistics": {"Stages": [{"WallClockTimeNs": 10, "Processors": 18446744073709551616.0}]}}]}'); -- { serverError BAD_ARGUMENTS }
SELECT planToASCII('{"Version": 1, "Root": "A", "Nodes": [{"NodeType": "T", "NodeId": "A", "Statistics": {"HashTable": {"Memory": "lots"}}}]}'); -- { serverError BAD_ARGUMENTS }

-- Numbers quoted as strings, as `toJSONString` writes them with `output_format_json_quote_64bit_integers = 1`,
-- render the same as unquoted ones, in the format of their metric.
SELECT planToASCII('{"Version": 1, "Root": "A", "MaxThreads": 2, "Nodes": [{"NodeType": "T", "NodeId": "A", "Statistics": {
    "HashTable": {"UniqueKeys": 40, "Memory": 4096},
    "Time": {"StepTimeNs": 1000, "StepTimeShare": 50, "BranchTimeNs": 2000, "BranchTimeShare": 100},
    "Stages": [{"Name": "", "WallClockTimeNs": 2000, "Processors": 2, "ProcessorTimeNs": {"Sum": 2000}}]}}]}') AS unquoted,
    unquoted = planToASCII('{"Version": "1", "Root": "A", "MaxThreads": "2", "Nodes": [{"NodeType": "T", "NodeId": "A", "Statistics": {
    "HashTable": {"UniqueKeys": "40", "Memory": "4096"},
    "Time": {"StepTimeNs": "1000", "StepTimeShare": 50, "BranchTimeNs": "2000", "BranchTimeShare": 100},
    "Stages": [{"Name": "", "WallClockTimeNs": "2000", "Processors": "2", "ProcessorTimeNs": {"Sum": "2000"}}]}}]}') AS same
FORMAT Vertical;

-- A plan logged by a real query renders its tree.
SELECT number FROM numbers(1000) WHERE number % 7 = 0 AND '05327_logged' != '' ORDER BY number DESC LIMIT 3 SETTINGS log_query_plans = 1 FORMAT Null;
SYSTEM FLUSH LOGS query_log;

SELECT
    count(),
    anyLast(startsWith(planToASCII(query_plan), 'Output: number\n\n')),
    anyLast(position(planToASCII(query_plan), '└──ReadFromSystemNumbers') > 0),
    anyLast(position(planToASCII(query_plan), 'I/O: rows ') > 0)
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND position(query, '05327_logged') > 0;

-- A plan logged by a read that analysed a projection shows it.
DROP TABLE IF EXISTS t_05327_projection;
CREATE TABLE t_05327_projection (k UInt64, d UInt64, PROJECTION p_by_d (SELECT * ORDER BY d)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_05327_projection SELECT number, number % 10 FROM numbers(1000);
SELECT count() FROM t_05327_projection WHERE d = 3 AND '05327_projection_read' != '' SETTINGS log_query_plans = 1, optimize_use_projections = 1, optimize_use_projection_filtering = 1 FORMAT Null;
SYSTEM FLUSH LOGS query_log;

SELECT
    count(),
    anyLast(position(planToASCII(query_plan), 'Projections:\n') > 0),
    anyLast(position(planToASCII(query_plan), 'Name: p_by_d\n') > 0)
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND position(query, '05327_projection_read') > 0;

DROP TABLE t_05327_projection;
