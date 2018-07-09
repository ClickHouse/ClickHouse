#!/bin/bash
set -e
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null && pwd)"
DOCS_DIR="${BASE_DIR}/.."
if [ $# -lt 1 ]; then
    LANGS="ru en"
elif [[ $# -eq 1 ]]; then
    LANGS=$1
fi

for lang in $LANGS; do
    cd ${DOCS_DIR}
    echo -e "\n\nLANG=$lang. Creating single page source"
    mkdir $lang'_single_page' 2>/dev/null || true
    cp -r $lang/images $lang'_single_page'
    ./tools/concatenate.py $lang
    echo -e "\n\nLANG=$lang. Building multipage..."
    mkdocs build -f mkdocs_$lang.yml
    echo -e "\n\nLANG=$lang. Building single page..."
    mkdocs build -f mkdocs_$lang'_single_page.yml'
    rm -rf $lang'_single_page'
done
