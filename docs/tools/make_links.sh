#!/bin/bash

# Fixes missing documentation in other languages
# by putting relative symbolic links to the original doc file.
# This is to be run from root of language directory, like "docs/en".

function do_make_links()
{
    langs=(en ru zh ja fa)
    src_file="$1"
    for lang in "${langs[@]}"
    do
        # replacing "/./" with /
        dst_file="../${lang}${src_file}"
        dst_file="${dst_file/\/\.\//\/}"
        dst_file="${dst_file/${lang}\./${lang}}"

        mkdir -p $(dirname "${dst_file}")
        ln -sr "${src_file}" "${dst_file}" 2>/dev/null
    done
}

export -f do_make_links
find . -iname '*.md' -exec /bin/bash -c 'do_make_links "{}"' \;
