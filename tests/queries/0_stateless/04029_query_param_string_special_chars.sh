#!/usr/bin/env bash
# Bug: named String query parameters containing tab (0x09) or newline (0x0a) bytes
# fail via the native TCP protocol with "isn't parsed completely".
#
# Root cause: Settings::toNameToNameMap() fully decodes Field dump values to raw
# bytes via readQuoted, but visitQueryParameter then calls deserializeTextEscaped
# which treats raw tab/newline as TSV field delimiters, stopping the parse early.
#
# Workaround: double-escape the character (e.g. pass \\t instead of a literal tab).
# The extra backslash survives the Field dump round-trip as a single backslash,
# giving deserializeTextEscaped the two-char \t sequence it expects.
#
# The same bug exists for the HTTP interface when using %09 or %0A URL-encoding.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Baseline: a String parameter with no special characters works fine.
$CLICKHOUSE_CLIENT --param_s='hello world' -q "SELECT {s:String}"

# Workaround: \\t (double-escaped backslash+t in bash $'...' syntax = literal \t on
# the wire). The server decodes \\ -> \, leaving \t in query_parameters, which
# deserializeTextEscaped then correctly converts to a tab. Length of the result
# is 11: len("hello") + 1 (tab) + len("world").
$CLICKHOUSE_CLIENT --param_s=$'hello\\tworld' -q "SELECT length({s:String})"

# Bug: literal tab (0x09) -- fails because raw 0x09 stops deserializeTextEscaped.
$CLICKHOUSE_CLIENT --param_s=$'hello\tworld' -q "SELECT {s:String}" 2>&1 | grep -om 1 "isn't parsed completely"

# Bug: literal newline (0x0a) -- same root cause, same failure.
$CLICKHOUSE_CLIENT --param_s=$'hello\nworld' -q "SELECT {s:String}" 2>&1 | grep -om 1 "isn't parsed completely"

# The following two cases should pass once the bug is fixed.
# A literal tab passed as a String parameter should round-trip and be returned as-is.
$CLICKHOUSE_CLIENT --param_s=$'hello\tworld' -q "SELECT {s:String}"
# A literal newline passed as a String parameter should round-trip and be returned as-is.
$CLICKHOUSE_CLIENT --param_s=$'hello\nworld' -q "SELECT {s:String}"
