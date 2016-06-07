#!/bin/sh

rm -rf re2_st
mkdir -p re2_st

for i in filtered_re2.h re2.h set.h stringpiece.h variadic_function.h;
do
    cp $1/re2/$i re2_st/$i
    sed -i -r 's/using re2::RE2;//g;s/namespace re2/namespace re2_st/g;s/re2::/re2_st::/g;s/\"re2\//\"re2_st\//g;s/(.*?_H)/\1_ST/g' re2_st/$i;
done
