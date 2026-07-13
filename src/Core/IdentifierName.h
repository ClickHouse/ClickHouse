#pragma once

#include <base/types.h>

#include <string_view>
#include <vector>

namespace DB
{

/// How a single identifier part was written in the query text.
/// Double quotes pin the part to exact-case matching in `standard` name matching mode;
/// unquoted and backtick-quoted parts are subject to ASCII case folding.
enum class IdentifierPartQuote : UInt8
{
    Unquoted,
    DoubleQuoted,
    Backticked,
};

/// Name matching policy. `Standard` is SQL-standard-like: every identifier part that is not
/// double-quoted matches through a single ASCII-folded namespace, and an exact spelling gets
/// no priority over other case variants.
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

/// A structured identifier name: part boundaries and per-part quoting are semantic,
/// so equality, hashing, formatting and serialization must all derive from this value
/// rather than from a flattened dot-joined string.
struct IdentifierName
{
    std::vector<IdentifierPart> parts;

    IdentifierName() = default;
    explicit IdentifierName(std::vector<IdentifierPart> parts_) : parts(std::move(parts_)) {}
    /// All parts unquoted.
    explicit IdentifierName(const std::vector<String> & spellings);

    bool empty() const { return parts.empty(); }
    size_t size() const { return parts.size(); }

    /// Flattened form for display and error messages only. Never re-split it.
    String toString() const;

    bool operator==(const IdentifierName & rhs) const = default;
};

}
