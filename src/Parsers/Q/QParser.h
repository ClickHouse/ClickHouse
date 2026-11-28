#pragma once

#include <Parsers/Q/QSyntax.h>

#include <string_view>


namespace DB
{

/// Parses a q program into its syntax tree. Throws `SYNTAX_ERROR` on malformed input and
/// `TOO_DEEP_RECURSION` when the nesting is deeper than `max_depth` (0 means no limit).
///
/// q is evaluated right to left without operator precedence: `2*3+4` is `2*(3+4)`. A verb between
/// two nouns is applied infix, a noun followed by a noun is a function application (`count x`),
/// and the q-sql templates (`select ... by ... from ... where ...`) are parsed as special forms.
QProgram parseQProgram(std::string_view text, size_t max_depth);

}
