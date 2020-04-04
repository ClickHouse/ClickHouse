#!/usr/bin/env bash
# Usage: replace-with-translation.sh <target_language> <path>
set -e
BASE_DIR=$(dirname $(readlink -f $0))
TEMP_FILE=$(mktemp)
trap 'rm -f -- "${TEMP_FILE}"' INT TERM HUP EXIT
TARGET_LANGUAGE="$1"
export INPUT="$2"
cat "${INPUT}" > "${TEMP_FILE}"
if [[ ! -z $SLEEP ]]
then
    sleep $[ ( $RANDOM % 20 )  + 1 ]s
fi
rm -f "${INPUT}"
mkdir -p $(dirname "${INPUT}") || true
YANDEX=1 "${BASE_DIR}/translate.sh" "${TARGET_LANGUAGE}" "${TEMP_FILE}" "${INPUT}"
git add "${INPUT}"
