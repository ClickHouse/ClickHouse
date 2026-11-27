#!/usr/bin/env bash

echo '%language=C++
%define class-name HTMLCharacterHash
%define lookup-function-name Lookup
%readonly-tables
%includes
%compare-strncmp
%global-table
%{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wimplicit-fallthrough"
#pragma GCC diagnostic ignored "-Wzero-as-null-pointer-constant"
#pragma GCC diagnostic ignored "-Wunused-macros"
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
#pragma GCC diagnostic ignored "-Wshorten-64-to-32"
// NOLINTBEGIN(google-runtime-int,hicpp-use-nullptr,modernize-use-nullptr,modernize-macro-to-enum)
%}
struct NameAndGlyph {
const char *name;
const char *glyph;
};
%%' > HTMLCharacterReference.gperf

# character reference as available at https://html.spec.whatwg.org/multipage/named-characters.html
curl -X GET https://html.spec.whatwg.org/entities.json |  jq -r 'keys[] as $k | "\"\($k)\", \(.[$k] | .characters|tojson)"' | sed 's/^"&/"/' >> HTMLCharacterReference.gperf
echo '%%' >> HTMLCharacterReference.gperf

# decodeHTMLComponent sizes its output buffer assuming an entity decodes to at most 6/5 of the bytes
# it occupies in the input ('&' + name, where name includes the trailing ';'). `&nGt;`/`&nLt;`
# (5 bytes -> 6-byte UTF-8) hit that bound exactly. Verify it at compile time over the whole table so
# a newer entity list cannot silently break the buffer sizing. (`%global-table` puts `wordlist` at
# file scope so this can reference it; the CMake target makes it `constexpr`.)
cat >> HTMLCharacterReference.gperf <<'CPP'

[[maybe_unused]] static constexpr bool html_entity_expansion_within_6_5 = []() constexpr
{
    for (const auto & entry : wordlist)
    {
        if (entry.glyph == nullptr)
            continue;
        size_t name_len = 0;
        while (entry.name[name_len] != '\0')
            ++name_len;
        size_t glyph_len = 0;
        while (entry.glyph[glyph_len] != '\0')
            ++glyph_len;
        if (glyph_len * 5 > (name_len + 1) * 6)
            return false;
    }
    return true;
}();
static_assert(
    html_entity_expansion_within_6_5,
    "An HTML entity decodes to more than 6/5 of its byte length; "
    "update the output buffer size in decodeHTMLComponent.cpp");
CPP

echo '// NOLINTEND(google-runtime-int,hicpp-use-nullptr,modernize-use-nullptr,modernize-macro-to-enum)' >> HTMLCharacterReference.gperf
