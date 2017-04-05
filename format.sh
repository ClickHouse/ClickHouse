#/usr/bin/env bash

clang_format=`bash -c "compgen -c clang-format | grep 'clang-format-[[:digit:]]' | sort --version-sort --reverse | head -n1"`
if [ ! -z $clang_format ]; then
    find dbms libs utils -name *.cpp -or -name *.h -exec $clang_format -i {} + ;
else
    echo clang-format missing. try to install:
    echo sudo apt install clang-format
    echo or
    echo sudo apt install clang-format-3.9
fi
