#!/usr/bin/env bash
# Usage: update-po.sh
set -ex
BASE_DIR=$(dirname $(readlink -f $0))
WEBSITE_DIR="${BASE_DIR}/../../../website"
LOCALE_DIR="${WEBSITE_DIR}/locale"
MESSAGES_POT="${LOCALE_DIR}/messages.pot"
BABEL_INI="${BASE_DIR}/babel-mapping.ini"
LANGS="en zh es fr ru ja tr fa"
source "${BASE_DIR}/venv/bin/activate"
cd "${WEBSITE_DIR}"
pybabel extract "." -o "${MESSAGES_POT}" -F "${BABEL_INI}"
for L in ${LANGS}
do
    pybabel update -d locale -l "${L}" -i "${MESSAGES_POT}" || \
    pybabel init -d locale -l "${L}" -i "${MESSAGES_POT}"
done
python3 "${BASE_DIR}/translate.py" po
for L in ${LANGS}
do
    pybabel compile -d locale -l "${L}"
done
