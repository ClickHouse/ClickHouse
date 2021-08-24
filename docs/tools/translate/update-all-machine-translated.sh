#!/usr/bin/env bash
BASE_DIR=$(dirname $(readlink -f $0))

function translate() {
    set -x
    LANGUAGE=$1
    DOCS_ROOT="${BASE_DIR}/../../"
    REV="$(git rev-parse HEAD)"
    for FILENAME in $(find "${DOCS_ROOT}${LANGUAGE}" -name "*.md" -type f)
    do
        HAS_MT_TAG=$(grep -c "machine_translated: true" "${FILENAME}")
        IS_UP_TO_DATE=$(grep -c "machine_translated_rev: \"${REV}\"" "${FILENAME}")
        if [ "${HAS_MT_TAG}" -eq "1" ] && [ "${IS_UP_TO_DATE}" -eq "0" ]
        then
            set -e
            EN_FILENAME=${FILENAME/\/${LANGUAGE}\///en/}
            rm "${FILENAME}" || true
            cp "${EN_FILENAME}" "${FILENAME}"
            DEBUG=1 SLEEP=1 ${BASE_DIR}/replace-with-translation.sh ${LANGUAGE} "${FILENAME}"
            set +e
        fi
    done
}
export BASE_DIR
export -f translate
parallel translate ::: es fr zh ja fa tr
