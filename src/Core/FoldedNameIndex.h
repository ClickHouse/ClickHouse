#pragma once

#include <Core/IdentifierName.h>

#include <map>
#include <set>

namespace DB
{

/// One shared index for resolving a name against a set of objects (databases, tables, ...).
/// Not thread-safe: the owner must hold the same lock it uses for the canonical object map.
class FoldedNameIndex
{
public:
    enum class Outcome : UInt8
    {
        Matched,
        Absent,
        Ambiguous,
    };

    struct ResolutionResult
    {
        Outcome outcome = Outcome::Absent;
        /// Canonical object name when Matched.
        String canonical;
        /// All matching canonical names, sorted, when Ambiguous.
        std::vector<String> candidates;
    };

    /// `pinned` objects (defined double-quoted in `standard` mode) are excluded from folded
    /// matching and can only be found by an exact-spelling lookup.
    void add(const String & canonical, bool pinned = false);
    void remove(const String & canonical);
    void rename(const String & old_canonical, const String & new_canonical, bool new_pinned = false);
    void clear();

    bool contains(const String & canonical) const { return objects.contains(canonical); }
    bool empty() const { return objects.empty(); }

    ResolutionResult resolve(const IdentifierPart & lookup, NameMatchMode mode) const;

private:
    /// Canonical name -> pinned. Ordered so ambiguity candidates come out sorted.
    std::map<String, bool> objects;
    /// Folded key -> canonical names of non-pinned objects.
    std::map<String, std::set<String>> folded_to_canonical;
};

}
