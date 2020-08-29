#!/usr/bin/env bash
# Usage: translate.sh <target_language> <input> <output>
set -e
BASE_DIR=$(dirname $(readlink -f $0))
OUTPUT=${3:-/dev/stdout}
export TARGET_LANGUAGE="$1"
export DEBUG
TEMP_FILE=$(mktemp)
export INPUT_PATH="$2"
INPUT_META="${INPUT_PATH}.meta"
INPUT_CONTENT="${INPUT_PATH}.content"

trap 'rm -f -- "${TEMP_FILE}" "${INPUT_META}" "${INPUT_CONTENT}"' INT TERM HUP EXIT
source "${BASE_DIR}/venv/bin/activate"

${BASE_DIR}/split_meta.py "${INPUT_PATH}"

pandoc "${INPUT_CONTENT}" --filter "${BASE_DIR}/filter.py" -o "${TEMP_FILE}" \
    -f "markdown-space_in_atx_header" -t "markdown_strict+pipe_tables+markdown_attribute+all_symbols_escapable+backtick_code_blocks+autolink_bare_uris-link_attributes+markdown_attribute+mmd_link_attributes-raw_attribute+header_attributes-grid_tables+definition_lists" \
    --atx-headers --wrap=none --columns=99999 --tab-stop=4
perl -pi -e 's/{\\#\\#/{##/g' "${TEMP_FILE}"
perl -pi -e 's/\\#\\#}/##}/g' "${TEMP_FILE}"
perl -pi -e 's/ *$//gg' "${TEMP_FILE}"
if [[ "${TARGET_LANGUAGE}" == "ru" ]]
then
    perl -pi -e 's/“/«/gg' "${TEMP_FILE}"
    perl -pi -e 's/”/»/gg' "${TEMP_FILE}"
fi
cat "${INPUT_META}" "${TEMP_FILE}" > "${OUTPUT}"
