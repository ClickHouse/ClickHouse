#!/usr/bin/env bash
# Regenerates minimal_dic.tar.gz from the sources in src/.
# Builds MeCab's dictionary compiler from the vendored contrib/MeCab sources, compiles the
# dictionary, and packs it reproducibly. Prints the SHA-256 — update ../configs/*.xml if it changes.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../../../.." && pwd)"
mecab="$root/contrib/MeCab/mecab/src"
config="$root/contrib/MeCab-cmake"
: "${CXX:=c++}"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

srcs=()
for f in "$mecab"/*.cpp; do
    case "$(basename "$f" .cpp)" in
        mecab|mecab-dict-gen|mecab-cost-train|mecab-system-eval|mecab-test-gen) continue ;;
        *) srcs+=("$f") ;;
    esac
done

"$CXX" -std=c++17 -O1 -w -Wno-register -DHAVE_CONFIG_H -DHAVE_ICONV -DICONV_CONST= \
    -I "$config" -I "$mecab" "${srcs[@]}" -lpthread -o "$work/mecab-dict-index"

mkdir -p "$work/out"
"$work/mecab-dict-index" -d "$here/src" -o "$work/out" -f UTF-8 -t UTF-8
cp "$here/src/dicrc" "$work/out/"

tar --sort=name --owner=0 --group=0 --numeric-owner --mtime='@0' \
    -C "$work/out" -cf "$work/dic.tar" sys.dic matrix.bin char.bin unk.dic dicrc
gzip -n -9 -c "$work/dic.tar" > "$here/minimal_dic.tar.gz"

echo "Wrote $here/minimal_dic.tar.gz"
sha256sum "$here/minimal_dic.tar.gz"
