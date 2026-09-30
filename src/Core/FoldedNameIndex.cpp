#include <Core/FoldedNameIndex.h>

namespace DB
{

void FoldedNameIndex::add(const String & canonical, bool pinned)
{
    auto [it, inserted] = objects.emplace(canonical, pinned);
    if (!inserted)
        return;
    if (!pinned)
        folded_to_canonical[foldIdentifierCaseASCII(canonical)].insert(canonical);
}

void FoldedNameIndex::remove(const String & canonical)
{
    auto it = objects.find(canonical);
    if (it == objects.end())
        return;
    if (!it->second)
    {
        auto folded_it = folded_to_canonical.find(foldIdentifierCaseASCII(canonical));
        if (folded_it != folded_to_canonical.end())
        {
            folded_it->second.erase(canonical);
            if (folded_it->second.empty())
                folded_to_canonical.erase(folded_it);
        }
    }
    objects.erase(it);
}

void FoldedNameIndex::rename(const String & old_canonical, const String & new_canonical, bool new_pinned)
{
    remove(old_canonical);
    add(new_canonical, new_pinned);
}

void FoldedNameIndex::clear()
{
    objects.clear();
    folded_to_canonical.clear();
}

FoldedNameIndex::ResolutionResult FoldedNameIndex::resolve(const IdentifierPart & lookup, NameMatchMode mode) const
{
    ResolutionResult result;

    const bool exact_only = mode == NameMatchMode::Sensitive || !lookup.isCaseFoldable();
    if (exact_only)
    {
        if (objects.contains(lookup.spelling))
        {
            result.outcome = Outcome::Matched;
            result.canonical = lookup.spelling;
        }
        return result;
    }

    auto folded_it = folded_to_canonical.find(foldIdentifierCaseASCII(lookup.spelling));
    if (folded_it == folded_to_canonical.end() || folded_it->second.empty())
        return result;

    const auto & matches = folded_it->second;
    if (matches.size() == 1)
    {
        result.outcome = Outcome::Matched;
        result.canonical = *matches.begin();
        return result;
    }

    result.outcome = Outcome::Ambiguous;
    result.candidates.assign(matches.begin(), matches.end());
    return result;
}

}
