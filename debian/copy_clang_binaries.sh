#!/bin/bash -e

# Копирует бинарник clang а также ld и shared-библиотеку libstdc++ в указанную директорию.
# Так повезло, что этого достаточно, чтобы затем собирать код на удалённом сервере с совпадающей версией Ubuntu, но без установленного компилятора.

DST=${1:-.};
PATH="/usr/local/bin:/usr/local/sbin:/usr/bin:$PATH"
LD=$(command -v gold || command -v ld.gold || command -v ld)

# Should be runned with correct path to clang
if [ -z "$CLANG" ]; then
    CLANG=$(which clang)
fi

if [ ! -x "$CLANG" ]; then
    echo "Not found executable clang."
    exit 1
fi

if [ ! -x "$LD" ]; then
    echo "Not found executable gold or ld."
    exit 1
fi

cp "$CLANG" "${DST}/clang"
cp "$LD" "${DST}/ld"

STDCPP=$(ldd $CLANG | grep -oE '/[^ ]+libstdc++[^ ]+')

[ -f "$STDCPP" ] && cp "$STDCPP" $DST
