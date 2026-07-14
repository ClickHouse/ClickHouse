#include <Analyzer/Resolve/TableExpressionData.h>

#include <DataTypes/IDataType.h>

#include <algorithm>

namespace DB
{

namespace
{

String joinPartSpellings(const IdentifierName & name)
{
    String result;
    for (const auto & part : name.parts)
    {
        if (!result.empty())
            result += '.';
        result += part.spelling;
    }
    return result;
}

/// The ASCII fold maps bytes one to one, so a candidate equal to the lookup under whole-string
/// folding has the same length with part boundaries at the same byte offsets. Verify that every
/// double-quoted part matches its candidate segment exactly.
bool candidateMatchesQuotedParts(const IdentifierName & name, std::string_view candidate)
{
    size_t offset = 0;
    for (const auto & part : name.parts)
    {
        if (!part.isCaseFoldable() && candidate.substr(offset, part.spelling.size()) != part.spelling)
            return false;
        offset += part.spelling.size() + 1;
    }
    return true;
}

}

void AnalysisTableExpressionData::ensureColumnMembershipSetsArePopulated() const
{
    if (column_membership_sets_populated)
        return;
    column_names.reserve(column_names_and_types.size());
    column_identifier_first_parts.reserve(column_names_and_types.size());
    for (const auto & column_name_and_type : column_names_and_types)
    {
        column_names.insert(column_name_and_type.name);
        Identifier column_name_identifier(column_name_and_type.name);
        column_identifier_first_parts.insert(column_name_identifier.at(0));
    }
    column_membership_sets_populated = true;
}

void AnalysisTableExpressionData::ensureFoldedColumnIndexIsPopulated() const
{
    if (folded_column_index_populated)
        return;

    for (const auto & column_name_and_type : column_names_and_types)
    {
        folded_column_names[foldIdentifierCaseASCII(column_name_and_type.name)].push_back(column_name_and_type.name);
        Identifier column_name_identifier(column_name_and_type.name);
        folded_column_first_parts.insert(foldIdentifierCaseASCII(column_name_identifier.at(0)));
    }

    for (auto & [_, canonical_names] : folded_column_names)
    {
        std::sort(canonical_names.begin(), canonical_names.end());
        canonical_names.erase(std::unique(canonical_names.begin(), canonical_names.end()), canonical_names.end());
    }

    folded_column_index_populated = true;
}

bool AnalysisTableExpressionData::canBindIdentifierStandard(const IdentifierName & name) const
{
    if (name.empty())
        return false;

    const auto & first_part = name.front();
    if (!first_part.isCaseFoldable())
    {
        ensureColumnMembershipSetsArePopulated();
        return column_identifier_first_parts.contains(first_part.spelling) || column_names.contains(first_part.spelling);
    }

    ensureFoldedColumnIndexIsPopulated();
    auto folded = foldIdentifierCaseASCII(first_part.spelling);
    if (folded_column_first_parts.contains(folded))
        return true;

    /// A backticked part may contain dots; bind through its first dotted segment,
    /// mirroring the `Identifier(name).at(0)` convention of the exact index.
    auto dot_pos = folded.find('.');
    return dot_pos != String::npos && folded_column_first_parts.contains(folded.substr(0, dot_pos));
}

AnalysisTableExpressionData::StandardMatchResult
AnalysisTableExpressionData::tryMatchColumnOrSubcolumnStandard(const IdentifierName & name) const
{
    StandardMatchResult result;
    if (name.empty())
        return result;

    ensureFoldedColumnIndexIsPopulated();

    auto collect_column_candidates = [&](const IdentifierName & column_part)
    {
        std::vector<String> verified;
        auto it = folded_column_names.find(foldIdentifierCaseASCII(joinPartSpellings(column_part)));
        if (it == folded_column_names.end())
            return verified;
        for (const auto & canonical : it->second)
            if (candidateMatchesQuotedParts(column_part, canonical))
                verified.push_back(canonical);
        return verified;
    };

    /// The whole name as a column name. No exact-spelling priority: with case-sibling
    /// columns every foldable spelling, including the exact one, is ambiguous.
    {
        auto candidates = collect_column_candidates(name);
        if (candidates.size() > 1)
        {
            result.outcome = StandardMatchResult::Outcome::Ambiguous;
            result.candidates = std::move(candidates);
            return result;
        }

        if (candidates.size() == 1)
        {
            const auto & node_map = getColumnNodeMap();
            auto it = node_map.find(candidates.front());
            if (it != node_map.end())
            {
                result.outcome = StandardMatchResult::Outcome::Matched;
                result.column_name = candidates.front();
                result.column_node = it->second;
                return result;
            }
        }
    }

    /// Split at part boundaries into a column prefix and a type-level subcolumn suffix,
    /// shortest column prefix first, mirroring the exact-mode split order.
    for (size_t split = 1; split < name.size(); ++split)
    {
        IdentifierName column_part(std::vector<IdentifierPart>(name.parts.begin(), name.parts.begin() + split));
        IdentifierName subcolumn_part(std::vector<IdentifierPart>(name.parts.begin() + split, name.parts.end()));

        auto column_candidates = collect_column_candidates(column_part);
        if (column_candidates.empty())
            continue;

        const auto & node_map = getColumnNodeMap();
        String subcolumn_joined = joinPartSpellings(subcolumn_part);
        String subcolumn_folded = foldIdentifierCaseASCII(subcolumn_joined);

        StandardMatchResult matched;
        size_t matches_count = 0;

        for (const auto & column_canonical : column_candidates)
        {
            auto it = node_map.find(column_canonical);
            if (it == node_map.end())
                continue;

            auto column_type = it->second->getResultType();

            std::vector<String> matched_subcolumns;
            for (const auto & subcolumn_name : column_type->getSubcolumnNames())
            {
                if (foldIdentifierCaseASCII(subcolumn_name) == subcolumn_folded
                    && candidateMatchesQuotedParts(subcolumn_part, subcolumn_name))
                    matched_subcolumns.push_back(subcolumn_name);
            }

            /// Dynamic subcolumns (e.g. JSON paths) are not enumerable. When folded enumeration
            /// finds nothing, fall back to the exact name: this can only add an exact binding.
            if (matched_subcolumns.empty() && column_type->tryGetSubcolumnType(subcolumn_joined))
                matched_subcolumns.push_back(subcolumn_joined);

            for (const auto & subcolumn_canonical : matched_subcolumns)
            {
                auto subcolumn_type = column_type->tryGetSubcolumnType(subcolumn_canonical);
                if (!subcolumn_type)
                    continue;

                ++matches_count;
                result.candidates.push_back(column_canonical + "." + subcolumn_canonical);
                matched.column_name = column_canonical;
                matched.subcolumn_name = subcolumn_canonical;
                matched.column_node = it->second;
                matched.subcolumn_type = subcolumn_type;
            }
        }

        if (matches_count > 1)
        {
            result.outcome = StandardMatchResult::Outcome::Ambiguous;
            std::sort(result.candidates.begin(), result.candidates.end());
            return result;
        }

        if (matches_count == 1)
        {
            matched.outcome = StandardMatchResult::Outcome::Matched;
            return matched;
        }

        result.candidates.clear();
    }

    return result;
}

const ColumnNameToColumnNodeMap & AnalysisTableExpressionData::getColumnNodeMap() const
{
    if (column_name_to_column_node.has_value())
        return *column_name_to_column_node;
    /// Emplace the (initially empty) map before invoking the populator. The populator
    /// first inserts every regular column (and ALIAS placeholders) into the map, then
    /// resolves ALIAS expressions; that resolution can recursively trigger identifier
    /// lookups that call this method again. Emplacing up front breaks the recursion:
    /// re-entrants find the map present and see the placeholders the populator has
    /// already inserted.
    auto & node_map = column_name_to_column_node.emplace();
    ensureColumnMembershipSetsArePopulated();
    if (populate_column_node_map)
        populate_column_node_map(node_map);
    return node_map;
}

void AnalysisTableExpressionData::setColumnNodeMapPopulator(std::function<void(ColumnNameToColumnNodeMap &)> populator)
{
    populate_column_node_map = std::move(populator);
}

ColumnNameToColumnNodeMap & AnalysisTableExpressionData::emplaceColumnNodeMap() const
{
    return column_name_to_column_node.emplace();
}

}
