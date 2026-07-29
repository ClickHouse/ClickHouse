#pragma once

#include <base/types.h>

#include <string_view>
#include <tuple>


namespace DB
{

/// Extracts a conservative fixed literal prefix from a ^-anchored regular expression.
/// Returns {fixed_prefix, is_perfect_prefix, is_exact}.
///
/// In regex, '^' means "must start at the beginning of the string".
/// This function walks the pattern after '^' and collects characters that
/// are guaranteed to appear, in order, at the start of every matching string.
/// It stops as soon as it hits any metacharacter or special construct where it
/// cannot guarantee a fixed character. The parser is conservative and may miss
/// some cases where a guaranteed fixed prefix could be derived but would be
/// complicated to do so. The result is a prefix that is common to all possible
/// matching strings.
///
/// `is_perfect_prefix` is true when the pattern matches every string starting with `fixed_prefix`,
/// so the pattern is equivalent to `startsWith(haystack, fixed_prefix)`, e.g. "^abc" or "^abc.*".
/// `is_exact` is true when the pattern matches only `fixed_prefix` itself, so it is equivalent to
/// an exact match, e.g. "^abc$"; in that case the prefix is always returned.
/// When `requires_perfect_prefix` is true and the prefix is neither perfect nor exact,
/// an empty prefix is returned even if a shorter non-perfect prefix could be extracted.
///
/// "^abc"
///   Every matching string starts with exactly "abc", and every string starting
///   with "abc" matches.
///   Prefix: "abc", perfect.
///
/// "^abc$"
///   '$' means "must end at the end of the string", so only "abc" matches.
///   Prefix: "abc", exact.
///
/// "^abc.*"
///   '.' means "any single character" and '*' means "zero or more times".
///   So after "abc" anything can follow.
///   Prefix: "abc", perfect.
///
/// "^abc.*def"
///   Only "abc" is guaranteed, and not every string starting with "abc" matches.
///   Prefix: "abc", not perfect.
///
/// "^abc\\|def"
///   A backslash before a special character removes its special meaning.
///   '|' normally means "or" (see below), but '\\|' means a literal '|'
///   character. So this matches strings starting with the text "abc|def".
///   Prefix: "abc|def", perfect.
///
/// "^abc\\d"
///   '\\d' means "any digit" (0-9). It is not a single fixed character,
///   so we stop. We can only guarantee "abc".
///   Prefix: "abc", not perfect.
///
/// "^abc|def"
///   '|' means "or" — match the left side or the right side.
///   This means: (string starts with "abc") OR (string contains "def" anywhere).
///   The right side has no '^', so it can match in the middle of a string.
///   We cannot guarantee any prefix at all.
///   Prefix: "".
///
/// "^abc[12]"
///   '[12]' is a character class — it matches either '1' or '2'.
///   Since the next character is not fixed, we stop at "abc".
///   Prefix: "abc", not perfect.
///
/// "^(abc-xx|abc-yy)"
///   An unescaped '|' inside a group gives two alternatives: the string starts
///   with "abc-xx" or "abc-yy". Both start with "abc-".
///   Prefix: "abc-", not perfect.
///
/// '(' is treated as a stop character (')' cannot appear unescaped in a
/// valid regex without a preceding '('). Patterns like
/// "^(abc)def" could theoretically yield prefix "abcdef", but analyzing
/// group semantics (optional groups, alternation inside groups, etc.)
/// is complex and error-prone. The only supported group is a top-level
/// alternation of plain literals, "^(branch1|branch2|...)$?", which is
/// handled separately.
std::tuple<String, bool, bool> extractFixedPrefixFromRegularExpression(std::string_view regexp, bool requires_perfect_prefix);

}
