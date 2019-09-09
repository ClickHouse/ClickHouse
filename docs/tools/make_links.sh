#!/bin/bash

# Fixes missing documentation in other languages
# by putting relative symbolic links to the original doc file.
# This is to be run from root of language directory, like "docs/en".

function do_make_links()
{
    langs=(en ru fa zh)
    src_file="$1"
    for lang in "${langs[@]}"
    do
        # replacing "/./" with /
        dst_file="../${lang}/${src_file}"
        dst_file="${dst_file/\/\.\//\/}"

        mkdir -p $(dirname "${dst_file}")
        ln -sr "${src_file}" "${dst_file}"
    done
}

export -f do_make_links
find . -iname '*.md' -exec /bin/bash -c 'do_make_links "{}"' \;
