#pragma once

#include <Analyzer/Resolve/IdentifierLookup.h>
#include <Core/IdentifierName.h>

#include <algorithm>

namespace DB
{

/// Quote-structured suffix of the lookup after `qualifier_parts` leading parts, for `standard`
/// name matching. Returns an empty name when folded matching must not be used: the mode is
/// `sensitive`, the lookup was synthesized from an internal name (no quote structure), or every
/// remaining part is double-quoted and therefore matches exactly anyway.
inline IdentifierName getFoldableIdentifierSuffix(const IdentifierLookup & identifier_lookup, size_t qualifier_parts, NameMatchMode mode)
{
    if (mode != NameMatchMode::Standard)
        return {};

    const auto & name = identifier_lookup.identifier_name;
    if (name.size() != identifier_lookup.identifier.getPartsSize() || name.size() <= qualifier_parts)
        return {};

    IdentifierName suffix(std::vector<IdentifierPart>(name.parts.begin() + qualifier_parts, name.parts.end()));

    bool any_foldable = false;
    for (const auto & part : suffix.parts)
        any_foldable |= part.isCaseFoldable();

    if (!any_foldable)
        return {};

    return suffix;
}

/// Single-part variant for names that are stored as a plain string with a quote flag
/// (CTE names, window names). Empty when folded matching must not be used.
inline IdentifierName getFoldableSingleName(const String & spelling, IdentifierPartQuote quote, NameMatchMode mode)
{
    if (mode != NameMatchMode::Standard || quote == IdentifierPartQuote::DoubleQuoted)
        return {};
    return IdentifierName({IdentifierPart{spelling, quote}});
}

/// Whether one identifier part matches the canonical name `canonical` under `standard`
/// matching. A `pinned` (double-quoted) definition is excluded from folded matching and
/// can only be found by a double-quoted exact-spelling reference.
inline bool identifierPartMatchesName(const IdentifierPart & part, const String & canonical, bool pinned = false)
{
    if (!part.isCaseFoldable())
        return part.spelling == canonical;
    if (pinned)
        return false;
    return foldIdentifierCaseASCII(part.spelling) == foldIdentifierCaseASCII(canonical);
}

/// Sorted canonical names from `names` matching `name` under `standard` folding with quoted pins.
template <typename Range>
std::vector<String> collectFoldedNameMatchesInNames(const Range & names, const IdentifierName & name)
{
    std::vector<String> matches;
    for (const auto & candidate : names)
        if (name.matchesFolded(candidate))
            matches.push_back(candidate);
    std::sort(matches.begin(), matches.end());
    return matches;
}

/// Scan a definition map keyed by name for entries matching `name` under `standard` folding.
/// `is_pinned(key, value)` excludes double-quoted definitions from folded matching. Returns the
/// sorted canonical names of all matches; more than one means the reference is ambiguous.
template <typename Map, typename IsPinnedFn>
std::vector<String> collectFoldedNameMatches(const Map & map, const IdentifierName & name, IsPinnedFn && is_pinned)
{
    std::vector<String> matches;
    String folded_key = name.foldedFullKey();

    for (const auto & [key, value] : map)
    {
        if (is_pinned(key, value))
            continue;
        if (foldIdentifierCaseASCII(key) != folded_key)
            continue;
        if (!name.quotedPartsMatch(key))
            continue;
        matches.push_back(key);
    }

    std::sort(matches.begin(), matches.end());
    return matches;
}

/// Existing foldable definition whose name is a case sibling of the foldable `new_name`
/// (folded-equal, different spelling), or nullptr. Used to reject case-colliding unquoted
/// definitions in one scope at registration, mirroring the CREATE-side contract.
template <typename Map, typename IsPinnedFn>
const String * findCaseSiblingName(const Map & map, const String & new_name, IsPinnedFn && is_pinned)
{
    String folded_key = foldIdentifierCaseASCII(new_name);

    for (const auto & [key, value] : map)
    {
        if (key == new_name || is_pinned(key, value))
            continue;
        if (foldIdentifierCaseASCII(key) == folded_key)
            return &key;
    }

    return nullptr;
}

}
