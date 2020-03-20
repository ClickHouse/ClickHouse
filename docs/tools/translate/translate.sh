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
    -f "markdown-space_in_atx_header" -t "markdown_strict+pipe_tables+markdown_attribute+all_symbols_escapable+backtick_code_blocks+autolink_bare_uris-link_attributes+markdown_attribute+mmd_link_attributes-raw_attribute+header_attributes-grid_tables" \
    --atx-headers --wrap=none --columns=99999 --tab-stop=2
perl -pi -e 's/{\\#\\#/{##/g' "${TEMP_FILE}"
perl -pi -e 's/\\#\\#}/##}/g' "${TEMP_FILE}"
perl -pi -e 's/ *$//gg' "${TEMP_FILE}"
if [[ "${TARGET_LANGUAGE}" -eq "ru" ]]
then
    perl -pi -e 's/“/«/gg' "${TEMP_FILE}"
    perl -pi -e 's/”/»/gg' "${TEMP_FILE}"
fi
cat "${TEMP_FILE}" > "${OUTPUT}"
