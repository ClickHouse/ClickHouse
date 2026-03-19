#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Create test znodes for path completion tests
path="/test-keeper-autocomplete-$CLICKHOUSE_DATABASE"
$CLICKHOUSE_KEEPER_CLIENT -q "rmr '$path'" >& /dev/null

$CLICKHOUSE_KEEPER_CLIENT -q "create '$path' 'x'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/dir_node' 'x'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/dir_node/child1' 'x'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/dir_node/child2' 'x'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/leaf_node' 'x'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/unique_prefix_xyz' 'x'"

# Create a znode with a space in the name (using quoted path)
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/space node' 'x'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/space node/inner' 'x'"

# Create a znode with a double quote in the name
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/has\"quote' 'dqval'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/has\"quote/deep' 'dqdeep'"

# Create a znode with a unique prefix and an embedded double quote for completion tests
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/dq\"node' 'dqn'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/dq\"node/inner' 'dqni'"

# Test bare (unquoted) path parsing: create and get without any quoting
echo -n "bare_path_parse: "
$CLICKHOUSE_KEEPER_CLIENT -q "create $path/bare_test test_value"
result=$($CLICKHOUSE_KEEPER_CLIENT -q "get $path/bare_test")
if [ "$result" = "test_value" ]; then echo "OK"; else echo "FAIL ($result)"; fi

# Test quoted path parsing still works
echo -n "quoted_path_parse: "
$CLICKHOUSE_KEEPER_CLIENT -q "create \"$path/quoted_test\" quoted_value"
result=$($CLICKHOUSE_KEEPER_CLIENT -q "get \"$path/quoted_test\"")
if [ "$result" = "quoted_value" ]; then echo "OK"; else echo "FAIL ($result)"; fi

# Test that trailing garbage after a valid command is rejected without executing.
# "create /path/x val garbage" must not create the node.
echo -n "trailing_garbage_rejected: "
stderr=$($CLICKHOUSE_KEEPER_CLIENT -q "create $path/garbage_test val garbage" 2>&1 >/dev/null)
if ! echo "$stderr" | grep -q "Syntax error"; then
    echo "FAIL (no syntax error, stderr: $stderr)"
elif $CLICKHOUSE_KEEPER_CLIENT -q "exists $path/garbage_test" 2>/dev/null | grep -q "1"; then
    echo "FAIL (node was created despite trailing garbage)"
else
    echo "OK"
fi

# Test that semicolons still allow multi-statement input.
echo -n "multi_statement_semicolon: "
$CLICKHOUSE_KEEPER_CLIENT -q "create $path/multi_a aa; create $path/multi_b bb"
result_a=$($CLICKHOUSE_KEEPER_CLIENT -q "get $path/multi_a")
result_b=$($CLICKHOUSE_KEEPER_CLIENT -q "get $path/multi_b")
if [ "$result_a" = "aa" ] && [ "$result_b" = "bb" ]; then echo "OK"; else echo "FAIL (a=$result_a b=$result_b)"; fi

# Test backslash-escaped space in path
echo -n "escaped_space_parse: "
result=$($CLICKHOUSE_KEEPER_CLIENT -q "get $path/space\ node")
if [ "$result" = "x" ]; then echo "OK"; else echo "FAIL ($result)"; fi

# Test that \\ followed by space is parsed as literal backslash + argument break.
# create /path/node\\ value  →  path="/path/node\", value="value"
echo -n "escaped_backslash_before_space: "
# Shell: \\\\ → \\, keeper-client: \\ → literal \, then space = word break
$CLICKHOUSE_KEEPER_CLIENT -q "create $path/bs_node\\\\ bs_value"
# Use quoted path for get to avoid bare-path escaping issues
result=$($CLICKHOUSE_KEEPER_CLIENT -q "get \"$path/bs_node\\\\\"")
if [ "$result" = "bs_value" ]; then echo "OK"; else echo "FAIL ($result)"; fi

# Test that \<space><space> is parsed as one escaped space + word break.
# "set /path/node\<sp><sp>new_value" → path="/path/node " (trailing space), value="new_value"
# Without the fix, the parser swallows both spaces and concatenates "new_value" into the path.
echo -n "escaped_space_then_break: "
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/trailing_sp ' 'old'"
$CLICKHOUSE_KEEPER_CLIENT -q "set $path/trailing_sp\  new_value"
result=$($CLICKHOUSE_KEEPER_CLIENT -q "get '$path/trailing_sp '")
if [ "$result" = "new_value" ]; then echo "OK"; else echo "FAIL ($result)"; fi

# Round-trip test: create a node whose name has both a space and a backslash,
# run ls to get the escaped representation, then feed that token back
# into get to verify the parser can consume what ls emits.
echo -n "ls_roundtrip_space_backslash: "
# Node name: a\b c  (literal backslash + space).
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/a\\\\b c' 'roundtrip'"
# ls emits: a\\b\ c  (backslash-escaped, no quotes since no quote chars in name).
# Use grep -oF with $'...' to get the exact literal bytes.
token=$($CLICKHOUSE_KEEPER_CLIENT -q "ls '$path'" | grep -oF $'a\\\\b\\ c')
# Feed the token back into get — the parser must reconstruct the original name.
result=$($CLICKHOUSE_KEEPER_CLIENT -q "get $path/$token")
if [ "$result" = "roundtrip" ]; then echo "OK"; else echo "FAIL (token=$token result=$result)"; fi

# Round-trip test for a node with backslashes but no spaces.
echo -n "ls_roundtrip_backslash_only: "
# Node name: x\y  (literal backslash, no space)
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/x\\\\y' 'bsonly'"
# ls emits: x\\y  (backslash-escaped). Extract with grep -oF.
token=$($CLICKHOUSE_KEEPER_CLIENT -q "ls '$path'" | grep -oF $'x\\\\y')
result=$($CLICKHOUSE_KEEPER_CLIENT -q "get $path/$token")
if [ "$result" = "bsonly" ]; then echo "OK"; else echo "FAIL (token=$token result=$result)"; fi

# Create a node with an embedded single quote and space: has'quote d
$CLICKHOUSE_KEEPER_CLIENT -q "create '${path}/has''quote d' 'x'"

# Round-trip test for a node with an embedded single quote and space.
# ls emits 'has\'quote d', the parser must handle \' inside single quotes.
echo -n "ls_roundtrip_single_quote: "
token=$($CLICKHOUSE_KEEPER_CLIENT -q "ls '$path'" | grep -oF $'\'has\\\'quote d\'')
result=$($CLICKHOUSE_KEEPER_CLIENT -q "get $path/$token")
if [ "$result" = "x" ]; then echo "OK"; else echo "FAIL (token=$token result=$result)"; fi

# Round-trip test for a node whose name contains a single quote but no space.
# Without the fix, ls would print it bare (it's), breaking the parser.
echo -n "ls_roundtrip_bare_quote: "
$CLICKHOUSE_KEEPER_CLIENT -q "create '${path}/it''s' 'quoteval'"
# ls emits 'it\'s' — use grep -oF with the literal bytes via $'...' ANSI-C quoting
token=$($CLICKHOUSE_KEEPER_CLIENT -q "ls '$path'" | grep -oF $'\'it\\\'s\'')
result=$($CLICKHOUSE_KEEPER_CLIENT -q "get $path/$token")
if [ "$result" = "quoteval" ]; then echo "OK"; else echo "FAIL (token=$token result=$result)"; fi

# Test that ls escapes node names containing spaces
echo -n "ls_escapes_spaces: "
result=$($CLICKHOUSE_KEEPER_CLIENT -q "ls '$path'")
# ls uses backslash-escaping for names with spaces (no quotes in name)
if echo "$result" | grep -qF 'space\ node'; then echo "OK"; else echo "FAIL ($result)"; fi

# Test that ls escapes embedded ' with backslash inside single-quoted output
echo -n "ls_escapes_single_quotes: "
result=$($CLICKHOUSE_KEEPER_CLIENT -q "ls '$path'")
# ls should output: 'has\'quote d'  (embedded ' escaped with backslash)
if echo "$result" | grep -qF "'has\'quote d'"; then echo "OK"; else echo "FAIL ($result)"; fi

# Round-trip test for a node with a semicolon (tokenizer-special character).
# Without quoting, the parser would stop at ';' treating it as a statement separator.
echo -n "ls_roundtrip_semicolon: "
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/a;b' 'semival'"
token=$($CLICKHOUSE_KEEPER_CLIENT -q "ls '$path'" | grep -oF $'\'a;b\'')
result=$($CLICKHOUSE_KEEPER_CLIENT -q "get $path/$token")
if [ "$result" = "semival" ]; then echo "OK"; else echo "FAIL (token=$token result=$result)"; fi

# Round-trip test for a node with an embedded tab character.
# ls must quote it (tab is a word break for the parser), so feeding the
# ls output back into get reconstructs the original name.
echo -n "ls_roundtrip_tab: "
# '\t' inside single-quoted string is interpreted as a tab by ClickHouse's
# ParserStringLiteral (which handles C-style escape sequences, unlike standard SQL).
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/a\tb' 'tabval'"
# ls should emit 'a<TAB>b' (quoted because of the tab).
# Extract the token; the literal tab is $'\t' in shell.
token=$($CLICKHOUSE_KEEPER_CLIENT -q "ls '$path'" | grep -oF $'\'a\tb\'')
result=$($CLICKHOUSE_KEEPER_CLIENT -q "get $path/$token")
if [ "$result" = "tabval" ]; then echo "OK"; else echo "FAIL (token=$token result=$result)"; fi

python3 "$CUR_DIR"/03988_keeper_client_autocomplete.python "$path"

$CLICKHOUSE_KEEPER_CLIENT -q "rmr '$path'" >& /dev/null
