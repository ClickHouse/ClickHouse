#/usr/bin/env bash

clang_format=`bash -c "compgen -c clang-format | grep 'clang-format-[[:digit:]]' | sort --version-sort --reverse | head -n1"`
find dbms libs utils -name *.cpp -or -name *.h -exec $clang_format -i {} + ;
