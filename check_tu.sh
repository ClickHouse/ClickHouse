#!/usr/bin/env bash
# Syntax-checks translation units of this worktree against the canonical build directory.
# For files that do not exist in the canonical checkout yet, the compile command of a sibling
# translation unit in the same directory is reused.
# Usage: ./check_tu.sh src/Storages/StorageMySQL.cpp [...]
set -u

WT=/home/ubuntu/ch-mysql-ssl
CANON=/home/ubuntu/ClickHouse
BUILD=$CANON/build_release
CC_JSON=$BUILD/compile_commands.json

fail=0
for rel in "$@"; do
    read -r template cmd < <(python3 - "$CC_JSON" "$CANON" "$rel" <<'EOF'
import json, os, sys
db = json.load(open(sys.argv[1]))
canon, rel = sys.argv[2], sys.argv[3]
target = os.path.join(canon, rel)
directory = os.path.dirname(target)
fallback = None
for entry in db:
    if entry["file"] == target:
        print(target, entry["command"])
        break
    if fallback is None and os.path.dirname(entry["file"]) == directory:
        fallback = entry
else:
    if fallback is None:
        sys.exit(1)
    print(fallback["file"], fallback["command"])
EOF
)
    if [ -z "${cmd:-}" ]; then
        echo "SKIP (no compile command available): $rel"
        fail=1
        continue
    fi

    # Drop the object output, compile this worktree's copy instead, and put this worktree's include
    # directories first so that its headers win over the canonical checkout's.
    cmd=$(echo "$cmd" | sed -E "s# -o [^ ]+\.o##; s#-c ${template}#-fsyntax-only ${WT}/${rel}#")
    # The worktree's include directories have to come before every -I of the canonical checkout,
    # so they are inserted right after the compiler binary.
    compiler=${cmd%% *}
    cmd="$compiler -I$WT/src -I$WT/base -I$WT/base/pcg-random -I$WT/src/Common/mysqlxx ${cmd#* }"

    out=$( (cd "$BUILD" && eval "$cmd") 2>&1 )
    if [ -n "$out" ]; then
        echo "=== $rel"
        echo "$out" | head -40
        fail=1
    else
        echo "OK   $rel"
    fi
done
exit $fail
