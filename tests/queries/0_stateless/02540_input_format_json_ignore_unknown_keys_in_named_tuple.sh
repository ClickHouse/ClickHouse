#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# This are just simple tests
echo '{"t" : {"a" : 1, "b" : 2}}' | $CLICKHOUSE_LOCAL --input_format_json_ignore_unknown_keys_in_named_tuple=0 --input-format=NDJSON --structure='t Tuple(a UInt32)' -q "select * from table" |& grep -m1 -o INCORRECT_DATA
echo '{"t" : {"a" : 1, "b" : 2}}' | $CLICKHOUSE_LOCAL --input_format_json_ignore_unknown_keys_in_named_tuple=1 --input-format=NDJSON --structure='t Tuple(a UInt32)' -q "select * from table"
echo '{"t" : {"b" : 2, "a" : 1}}' | $CLICKHOUSE_LOCAL --input_format_json_ignore_unknown_keys_in_named_tuple=0 --input-format=NDJSON --structure='t Tuple(a UInt32)' -q "select * from table" |& grep -m1 -o NOT_FOUND_COLUMN_IN_BLOCK
echo '{"t" : {"b" : 2, "a" : 1}}' | $CLICKHOUSE_LOCAL --input_format_json_ignore_unknown_keys_in_named_tuple=1 --input-format=NDJSON --structure='t Tuple(a UInt32)' -q "select * from table"

# And now let's try to parse something more complex - gharchive.
# (see https://github.com/ClickHouse/ClickHouse/issues/15323)
#
# NOTE: That JSON here was simplified
gharchive_structure=(
    "type String,"
    "actor Tuple(login String),"
    "repo Tuple(name String),"
    "created_at DateTime('UTC'),"
    "payload Tuple(
         updated_at DateTime('UTC'),
         action String,
         comment Tuple(
             id UInt64,
             path String,
             position UInt32,
             line UInt32,
             user Tuple(
                 login String
             ),
             diff_hunk String,
             original_position UInt32,
             commit_id String,
             original_commit_id String
         ),
         review Tuple(
             body String,
             author_association String,
             state String
         ),
         ref String,
         ref_type String,
         issue Tuple(
             number UInt32,
             title String,
             labels Nested(
                 name String
             ),
             state String,
             locked UInt8,
             assignee Tuple(
                 login String
             ),
             assignees Nested(
                 login String
             ),
             comment String,
             closed_at DateTime('UTC')
         ),
         pull_request Tuple(
             merged_at DateTime('UTC'),
             merge_commit_sha String,
             requested_reviewers Nested(
                 login String
             ),
             requested_teams Nested(
                 name String
             ),
             head Tuple(
                 ref String,
                 sha String
             ),
             base Tuple(
                 ref String,
                 sha String
             ),
             merged UInt8,
             mergeable UInt8,
             rebaseable UInt8,
             mergeable_state String,
             merged_by Tuple(
                 login String
             ),
             review_comments UInt32,
             maintainer_can_modify UInt8,
             commits UInt32,
             additions UInt32,
             deletions UInt32,
             changed_files UInt32
         ),
         size UInt32,
         distinct_size UInt32,
         member Tuple(
             login String
         ),
         release Tuple(
             tag_name String,
             name String
         )
     )"
)
gharchive_settings=(
    --date_time_input_format best_effort
    --input_format_json_ignore_unknown_keys_in_named_tuple 1
    --input-format JSONEachRow
    --output-format JSONObjectEachRow
)

$CLICKHOUSE_LOCAL "${gharchive_settings[@]}" --structure="${gharchive_structure[*]}" -q "select * from table" <<EOL
{"type":"CreateEvent","actor":{"login":"foobar"},"repo":{"name":"ClickHouse/ClickHouse"},"payload":{"ref":"backport","ref_type":"branch"},"created_at":"2023-01-26T10:48:02Z"}
EOL

# NOTE: due to [1] we cannot use dot.dot notation, only tupleElement()
#
#   [1]: https://github.com/ClickHouse/ClickHouse/issues/24607
$CLICKHOUSE_LOCAL --enable_analyzer=1 "${gharchive_settings[@]}" --structure="${gharchive_structure[*]}" -q "
    SELECT
        payload.issue.labels.name AS labels,
        payload.pull_request.merged_by.login AS merged_by
    FROM table
" <<EOL
{"type":"PullRequestEvent","actor":{"login":"foobar"},"repo":{"name":"ClickHouse/ClickHouse"},"payload":{"ref":"backport","ref_type":"branch","pull_request":{"merged_by":null}}}
{"type":"PullRequestEvent","actor":{"login":"foobar"},"repo":{"name":"ClickHouse/ClickHouse"},"payload":{"ref":"backport","ref_type":"branch","pull_request":{"merged_by":{"login": "foobar"}}}}
{"type":"IssueCommentEvent","actor":{"login":"foobar"},"repo":{"name":"ClickHouse/ClickHouse"},"payload":{"issue":{"labels":[]}}}
{"type":"IssueCommentEvent","actor":{"login":"foobar"},"repo":{"name":"ClickHouse/ClickHouse"},"payload":{"issue":{"labels":[{"name":"backport"}]}}}
EOL
