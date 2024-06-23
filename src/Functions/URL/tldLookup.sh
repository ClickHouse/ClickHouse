#!/usr/bin/env bash

[ ! -f public_suffix_list.dat ] && wget -nv -O public_suffix_list.dat https://publicsuffix.org/list/public_suffix_list.dat

echo '%language=C++
%define lookup-function-name isValid
%define class-name TopLevelDomainLookupHash
%readonly-tables
%includes
%compare-strncmp
%{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wimplicit-fallthrough"
#pragma GCC diagnostic ignored "-Wzero-as-null-pointer-constant"
#pragma GCC diagnostic ignored "-Wunused-macros"
%}
# List generated using https://publicsuffix.org/list/public_suffix_list.dat
%%' > tldLookup.gperf
grep -v "//" public_suffix_list.dat | grep . | grep "\." | grep -ve "\..*\..*" >> tldLookup.gperf
echo "%%" >> tldLookup.gperf
