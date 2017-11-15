#!/bin/sh
test -z "$1" && exit 1
echo "/* GENERATED */"
echo "#ifndef cursor_i_h"
echo "#define cursor_i_h"
sed -ne 's/^->"\(\S*\)" \(\d*\)/#define \1 \2/p' < $1 || exit $?
echo "#endif"
