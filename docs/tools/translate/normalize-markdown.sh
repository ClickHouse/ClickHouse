#!/usr/bin/env bash
# Usage: normalize-en-markdown.sh <path>
set -e
BASE_DIR=$(dirname $(readlink -f $0))
TEMP_FILE=$(mktemp)
trap 'rm -f -- "${TEMP_FILE}"' INT TERM HUP EXIT
INPUT="$1"
if [[ ! -L "${INPUT}" ]]
then
    export INPUT
    cat "${INPUT}" > "${TEMP_FILE}"
    "${BASE_DIR}/translate.sh" "en" "${TEMP_FILE}" "${INPUT}"
fi
