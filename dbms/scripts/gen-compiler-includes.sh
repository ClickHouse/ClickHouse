#!/bin/sh
#
# Generates the list of -isystem includes from gcc or clang compiler
# Usage: ./gen-includes.sh [COMPILER]
#

# Set locale to C to capture GCC's "search starts here" text in English
export LANG=C
export LC_ALL=C
export LC_MESSAGES=C
CC=$1 || cc
if test -z "$CC"; then CC=cc; fi

echo "" | $CC -x c++ -E -Wp,-v - 2>&1|
    sed -n '/#include <...> search starts here:/,/End of search list./p' |
    sed -e '1d;$d' -e 's/^[ \t ]*/" -isystem /;s/$/"/'
