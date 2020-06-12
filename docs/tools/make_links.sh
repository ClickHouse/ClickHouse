#!/bin/bash

# Fixes missing documentation in other languages
# by putting relative symbolic links to the original doc file.

BASE_DIR=$(dirname $(readlink -f $0))

function do_make_links()
{
    set -x
    langs=(en es zh fr ru ja tr fa)
    src_file="$1"
    for lang in "${langs[@]}"
    do
        dst_file="${src_file/\/en\///${lang}/}"
        mkdir -p $(dirname "${dst_file}")
        ln -sr "${src_file}" "${dst_file}" 2>/dev/null
    done
}

export -f do_make_links
find "${BASE_DIR}/../en" -iname '*.md' -exec /bin/bash -c 'do_make_links "{}"' \;
