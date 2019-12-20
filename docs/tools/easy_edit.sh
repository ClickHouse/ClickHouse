#!/usr/bin/env bash
# Creates symlinks to docs in ClickHouse/docs/edit/
# that are easy to open in both languages simultaneously
# for example, with `vim -O docs/edit/my_article/*`

set -ex
BASE_DIR="$(dirname $(readlink -f $0))/.."
DOCS_DIR="${BASE_DIR}"
EDIT_DIR="${BASE_DIR}/edit"

pushd "${DOCS_DIR}/en"
ARTICLES=$(find . -name '*.md' | sed -e 's/\.md$//g' -e 's/^\.\/en\///g')
popd

rm -rf "${EDIT_DIR}" || true

for DOCS_LANG in en ru zh ja fa
do
    for ARTICLE in ${ARTICLES}
    do
        ARTICLE_DIR="${EDIT_DIR}/${ARTICLE}"
        mkdir -p $ARTICLE_DIR || true
        ln -s "${DOCS_DIR}/${DOCS_LANG}/${ARTICLE}.md" "${ARTICLE_DIR}/${DOCS_LANG}.md"
    done
done


