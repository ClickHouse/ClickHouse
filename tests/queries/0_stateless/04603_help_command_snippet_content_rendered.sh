#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the `Avro` format needs the `avrocpp` contrib, which is not built into the Fast test.
# The `help` CLI command renders an entity's embedded documentation (`system.documentation`) in the
# terminal. Some pages converted from the website import a shared snippet (a settings table, a
# data-type mapping) and use it as a self-closing tag, e.g. `<PrettyFormatSettings/>`. On the website
# Mintlify expands the import; the terminal renderer has no Mintlify, so it resolves the known import
# to the snippet's content (see `DOC_SNIPPETS` / `resolveDocSnippets` in `TerminalMarkdownRenderer.cpp`).
#
# This drives the real render path end to end (`help <name>` -> system.documentation -> renderer) and
# asserts the *rendered* output actually contains the resolved snippet content and no longer contains
# the raw snippet tag. Unlike a grep of the page/binary source, it fails if the resolution stops
# happening -- not merely if the implementation text still ships.
# https://github.com/ClickHouse/ClickHouse/issues/89377

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The output of `clickhouse-local -q` is a (non-tty) pipe, so the rendering is deterministic plain text.

# check <name> <phrase-unique-to-the-snippet> <raw-tag>
check() {
    local name=$1 phrase=$2 tag=$3
    local out
    out=$($CLICKHOUSE_LOCAL -q "help $name")
    # The resolved content is present in the rendered page (flatten word-wrap before matching) ...
    printf '%s' "$out" | tr '\n' ' ' | tr -s ' ' | grep -qF "$phrase" \
        && echo "OK $name: resolved snippet content is rendered" \
        || echo "FAIL $name: missing resolved snippet content '$phrase'"
    # ... and the raw snippet tag is gone (it was resolved, not left literal or dropped with content).
    printf '%s' "$out" | grep -qF "$tag" \
        && echo "FAIL $name: raw snippet tag $tag> still present" \
        || echo "OK $name: raw snippet tag $tag> is resolved away"
}

check JSON      "is designed for querying, filtering, and aggregating" "<WhenToUseJson"
check Pretty    "output_format_pretty_max_rows"                        "<PrettyFormatSettings"
check RowBinary "format_binary_max_string_size"                        "<RowBinaryFormatSettings"
# The snippet is matched by its imported path, not the local binding: `Avro` imports the shared
# `data-types-matching.mdx` under the alias `DataTypeMapping`.
check Avro      "supported by the Apache Avro format"                  "<DataTypeMapping"
