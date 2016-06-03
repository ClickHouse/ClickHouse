#!/bin/sh

# First, compile and install Poco library with static libraries and -fPIC option enabled.

g++ \
    -std=c++11 \
    -Wall -Werror \
    -O2 \
    -g \
    -fPIC \
    -fvisibility-inlines-hidden \
    -shared \
    -Wl,-Bstatic,--whole-archive \
        *.cpp \
        -lPocoFoundation -lPocoNet \
        -static-libgcc -static-libstdc++ \
    -Wl,--no-whole-archive \
    -Wl,--version-script=linker_script \
    -o odbc.so
