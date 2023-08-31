#!/usr/bin/env bash

echo '%language=C++
%define class-name HTMLCharacterHash
%define lookup-function-name Lookup
%readonly-tables
%includes
%compare-strncmp
%{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wimplicit-fallthrough"
#pragma GCC diagnostic ignored "-Wzero-as-null-pointer-constant"
#pragma GCC diagnostic ignored "-Wunused-macros"
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
#pragma GCC diagnostic ignored "-Wshorten-64-to-32"
%}
struct NameAndGlyph {
const char *name;
const char *glyph;
};
%%' > HTMLCharacterReference.gperf

# character reference as available at https://html.spec.whatwg.org/multipage/named-characters.html
curl -X GET https://html.spec.whatwg.org/entities.json |  jq -r 'keys[] as $k | "\"\($k)\", \(.[$k] | .characters|tojson)"' | sed 's/^"&/"/' >> HTMLCharacterReference.gperf
echo '%%' >> HTMLCharacterReference.gperf

if ! command -V gperf &> /dev/null
then
  echo "error: gperf command not found. Install gperf to continue."
  exit 1
fi;

gperf -t --output-file=HTMLCharacterReference.generated.cpp HTMLCharacterReference.gperf