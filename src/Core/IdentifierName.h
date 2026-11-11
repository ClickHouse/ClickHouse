#pragma once

#include <base/types.h>

#include <string_view>
#include <vector>

namespace DB
{

/// How an identifier part was quoted. Double quotes pin the part to exact-case matching
/// in `standard` name matching mode; unquoted and backticked parts are ASCII case-folded.
enum class IdentifierPartQuote : UInt8
{
    Unquoted,
    DoubleQuoted,
    Backticked,
};

/// Name matching policy. In `Standard` (SQL-standard-like) mode every part that is not
/// double-quoted matches through a single ASCII-folded namespace; exact spelling gets no priority.
enum class NameMatchMode : UInt8
{
    Sensitive,
    Standard,
};

/// The single ASCII-only case folding used for identifier matching everywhere.
/// Non-ASCII bytes pass through unchanged.
String foldIdentifierCaseASCII(std::string_view name);

/// One component of a possibly compound identifier, with its quoting preserved.
struct IdentifierPart
{
    String spelling;
    IdentifierPartQuote quote = IdentifierPartQuote::Unquoted;

    bool isCaseFoldable() const { return quote != IdentifierPartQuote::DoubleQuoted; }

    /// Matching key in `standard` mode: folded when foldable, exact when double-quoted.
    String matchingKey() const;

    bool operator==(const IdentifierPart & rhs) const = default;
};

/// A structured identifier name. Part boundaries and per-part quoting are semantic:
/// equality, hashing and formatting must derive from them, not from a dot-joined string.
struct IdentifierName
{
    std::vector<IdentifierPart> parts;

    IdentifierName() = default;
    explicit IdentifierName(std::vector<IdentifierPart> parts_) : parts(std::move(parts_)) {}
    /// All parts unquoted.
    explicit IdentifierName(const std::vector<String> & spellings);

    bool empty() const { return parts.empty(); }
    size_t size() const { return parts.size(); }

    IdentifierPart & operator[](size_t i) { return parts[i]; }
    const IdentifierPart & operator[](size_t i) const { return parts[i]; }
    IdentifierPart & front() { return parts.front(); }
    const IdentifierPart & front() const { return parts.front(); }
    IdentifierPart & back() { return parts.back(); }
    const IdentifierPart & back() const { return parts.back(); }
    auto begin() const { return parts.begin(); }
    auto end() const { return parts.end(); }
    void push_back(IdentifierPart part) { parts.push_back(std::move(part)); }
    void pop_back() { parts.pop_back(); }
    void clear() { parts.clear(); }

    std::vector<String> spellings() const;

    bool anyPartDoubleQuoted() const;

    /// Matching key of the whole name in `standard` mode: ASCII fold of the dot-joined spellings.
    String foldedFullKey() const;

    /// Whether every double-quoted part matches its byte segment of `candidate` exactly. Valid only
    /// when `candidate` folds equal to this name: the fold is byte-wise, so offsets stay aligned.
    bool quotedPartsMatch(std::string_view candidate) const;

    /// `standard`-mode match against a canonical name: folded equality plus exact double-quoted parts.
    bool matchesFolded(std::string_view candidate) const;

    /// Flattened form for display and error messages only. Never re-split it.
    String toString() const;

    bool operator==(const IdentifierName & rhs) const = default;
};

}
