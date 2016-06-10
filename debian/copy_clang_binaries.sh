#!/bin/bash -e

# Копирует бинарник clang а также ld и shared-библиотеку libstdc++ в указанную директорию.
# Так повезло, что этого достаточно, чтобы затем собирать код на удалённом сервере с совпадающей версией Ubuntu, но без установленного компилятора.

DST=${1:-.};
PATH="/usr/local/bin:/usr/local/sbin:/usr/bin:$PATH"
CLANG=$(command -v clang)
LD=$(command -v gold || command -v ld.gold || command -v ld)

if [ ! -x "$CLANG" ]; then
	echo "Not found executable clang."
	exit 1
fi

if [ ! -x "$LD" ]; then
	echo "Not found executable gold or ld."
	exit 1
fi

cp "$CLANG" $DST
cp "$LD" ${DST}/ld

STDCPP=$(ldd $(command -v clang) | grep -oE '/[^ ]+libstdc++[^ ]+')

[ -f "$STDCPP" ] && cp "$STDCPP" $DST
