#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `clickhouse benchmark --queries-format script` splits its input with `splitMultipartQuery`,
# the same code path used by the PostgreSQL wire-protocol handler. A script that ends with a
# comment after the trailing semicolon used to fail with `Empty query` (SYNTAX_ERROR).

run_script() {
    $CLICKHOUSE_BENCHMARK --iterations 1 --queries-format script 2>&1 1>/dev/null | grep -F 'Exception' ||:
    echo "OK"
}

# Block comment after the trailing semicolon.
run_script <<'EOF'
SELECT 1;
/* trailing comment */
EOF

# Line comment after the trailing semicolon.
run_script <<'EOF'
SELECT 1;
-- trailing comment
EOF

# Several queries, the last one followed by a comment.
run_script <<'EOF'
SELECT 1;
SELECT 2;
/* trailing comment */
EOF

# Only whitespace and a comment after the semicolon (no newline before the comment).
run_script <<'EOF'
SELECT 1; /* trailing comment */
EOF
