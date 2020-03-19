#!/usr/bin/env bash
# Usage: translate.sh <target_language> <input> <output>
set -e
BASE_DIR=$(dirname $(readlink -f $0))
OUTPUT=${3:-/dev/stdout}
export TARGET_LANGUAGE="$1"
export DEBUG
TEMP_FILE=$(mktemp)
trap 'rm -f -- "${TEMP_FILE}"' INT TERM HUP EXIT
source "${BASE_DIR}/venv/bin/activate"
pandoc "$2" --filter "${BASE_DIR}/filter.py" -o "${TEMP_FILE}" \
    -f markdown -t "markdown_strict+pipe_tables+markdown_attribute+all_symbols_escapable+backtick_code_blocks" \
    --atx-headers --wrap=none
perl -pi -e 's/{\\#\\#/{##/g' "${TEMP_FILE}"
perl -pi -e 's/\\#\\#}/##}/g' "${TEMP_FILE}"
cat "${TEMP_FILE}" > "${OUTPUT}"
