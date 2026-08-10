#pragma once

#include <base/types.h>

#include <string_view>


namespace DB
{

/// A fixed literal prefix extracted from a regular expression: every matching string starts with `prefix`.
struct RegexpFixedPrefix
{
    String prefix;

    /// The regular expression matches every string starting with `prefix`, so it is equivalent to
    /// `startsWith(haystack, prefix)`, e.g. "^abc" or "^abc.*".
    bool is_perfect = false;

    /// The regular expression matches `prefix` and nothing else, so it is equivalent to an exact match,
    /// e.g. "^abc$". Such a prefix is not perfect, but it describes the matching strings even more precisely.
    bool is_exact = false;
};

/// Extracts a conservative fixed literal prefix from a ^-anchored regular expression: the characters
/// guaranteed to appear, in order, at the start of every matching string. The scan stops at the first
/// metacharacter where no fixed character can be guaranteed, so the prefix may be shorter than
/// theoretically possible. A pattern without a '^' anchor guarantees nothing and gives an empty prefix.
///
/// When `requires_perfect_prefix` is true and the prefix is neither perfect nor exact,
/// an empty prefix is returned even if a shorter non-perfect prefix could be extracted.
///
/// "^abc"             -> "abc", perfect: every string starting with "abc" matches.
/// "^abc$"            -> "abc", exact: only "abc" matches.
/// "^abc.*"           -> "abc", perfect: anything can follow "abc".
/// "^abc.*$"          -> "abc", perfect: '.' matches a newline too, so "abc\ndef" matches as well.
/// "^abc.*def"        -> "abc", not perfect: not every string starting with "abc" matches.
/// "^abc\\|def"       -> "abc|def", perfect: "\\|" is a literal '|', not an alternation.
/// "^abc\\d"          -> "abc", not perfect: "\\d" is any digit, not a fixed character.
/// "^abc[12]"         -> "abc", not perfect: '[12]' matches either '1' or '2'.
/// "^abc|def"         -> "", nothing is guaranteed: the right branch has no '^', so it can match
///                      in the middle of a string.
/// "^(abc-xx|abc-yy)" -> "abc-", not perfect: the longest prefix common to all branches.
///
/// '(' is treated as a stop character (')' cannot appear unescaped in a valid regex without a
/// preceding '('). Patterns like "^(abc)def" could theoretically yield prefix "abcdef", but analyzing
/// group semantics (optional groups, alternation inside groups, etc.) is complex and error-prone.
/// The only supported group is a top-level alternation of plain literals,
/// "^(branch1|branch2|...)$?", which is handled separately.
RegexpFixedPrefix extractFixedPrefixFromRegularExpression(std::string_view regexp, bool requires_perfect_prefix);

}
