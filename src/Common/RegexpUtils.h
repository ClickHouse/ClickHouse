#pragma once

#include <base/types.h>


namespace DB
{

/// Extracts a conservative fixed literal prefix from a ^-anchored regular expression.
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
/// "^abc"
///   Every matching string starts with exactly "abc".
///   Prefix: "abc".
///
/// "^abc.*"
///   '.' means "any single character" and '*' means "zero or more times".
///   So after "abc" anything can follow. We can only guarantee "abc".
///   Prefix: "abc".
///
/// "^abc\\|def"
///   A backslash before a special character removes its special meaning.
///   '|' normally means "or" (see below), but '\\|' means a literal '|'
///   character. So this matches strings starting with the text "abc|def".
///   Prefix: "abc|def".
///
/// "^abc\\d"
///   '\\d' means "any digit" (0-9). It is not a single fixed character,
///   so we stop. We can only guarantee "abc".
///   Prefix: "abc".
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
///   Prefix: "abc".
///
/// '(' is treated as a stop character (')' cannot appear unescaped in a
/// valid regex without a preceding '('). Patterns like
/// "^(abc)def" could theoretically yield prefix "abcdef", but analyzing
/// group semantics (optional groups, alternation inside groups, etc.)
/// is complex and error-prone. The alternation helper
/// `extractCommonPrefixFromAlternationBranches` handles the important case of
/// "^(branch1|branch2|...)" separately.
String extractFixedPrefixFromRegularExpression(const String & regexp);

/// Returns true if the expression contains any unescaped '|'.
bool expressionHasUnescapedAlternation(const String & expression);

/// Handles the simple alternation pattern "^(branch1|branch2|...)$?" where
/// each branch is a plain literal string (no metacharacters, no nesting).
///
/// This is called when the expression contains an unescaped '|', meaning
/// `extractFixedPrefixFromRegularExpression` cannot be used (it would stop
/// at '|' or '('). Returns empty for any pattern more complex than simple
/// literal branches inside a single group.
///
/// "^(abc-xx|abc-yy)"
///   The '|' gives two alternatives: the string starts with "abc-xx" or "abc-yy".
///   Both start with "abc-", so every matching string begins with "abc-".
///   Prefix: "abc-".
///
/// "^(abc-xx-1|abc-xx-2|abc-yy-1)"
///   Three alternatives. All three start with "abc-", but they diverge
///   after that ('x' vs 'y'). So "abc-" is the longest common start.
///   Prefix: "abc-".
///
/// "^(abc|def)"
///   Two alternatives: "abc" and "def". They share nothing at the start —
///   'a' vs 'd' already differ. We cannot guarantee any prefix.
///   Prefix: "".
///
/// "^(abc|def)$"
///   '$' means "must end at the end of the string". It constrains what
///   comes after the match, but does not change what the string starts
///   with. So the prefix analysis is the same as without '$'.
///   Prefix: "".
///
/// Not supported (returns empty — could be improved in the future):
///
/// "^(abc.*|abd.*)"
///   Branches contain '.*' (wildcard). We only handle plain literal branches.
///   The common prefix "ab" could theoretically be extracted, but is not.
///   Prefix: "".
///
/// "^(abc|abd)+"
///   The '+' after the group means it must appear at least once.
///   We only handle patterns where the group is followed by '$' or end
///   of expression. Prefix: "".
///
/// "^(abc(1|2)|abc(3|4))"
///   Branches contain nested groups. We only handle flat literal branches.
///   The common prefix "abc" could theoretically be extracted, but is not.
///   Prefix: "".
String extractCommonPrefixFromAlternationBranches(const String & expression);

}
