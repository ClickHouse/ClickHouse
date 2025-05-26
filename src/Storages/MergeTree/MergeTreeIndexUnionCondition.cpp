#include <Storages/MergeTree/MergeTreeIndexUnionCondition.h>
#include <Common/Exception.h>
#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void MergeTreeIndexUnionCondition::addIndex(const MergeTreeIndexPtr & index)
{
    if (index->getGranularity() != granularity)
        throw Exception(ErrorCodes::LOGICAL_ERROR, 
            "Index {} has different granularity ({}) than expected ({})", 
            index->index.name, index->getGranularity(), granularity);

    indices_with_conditions.push_back({index, nullptr});
}

void MergeTreeIndexUnionCondition::setCondition(size_t index_pos, MergeTreeIndexConditionPtr condition)
{
    if (index_pos >= indices_with_conditions.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, 
            "Index position {} is out of range ({})", 
            index_pos, indices_with_conditions.size());
    
    indices_with_conditions[index_pos].condition = condition;
}

bool MergeTreeIndexUnionCondition::alwaysUnknownOrTrue() const
{
    // Union condition is unknown/true only if ALL indexes are unknown/true
    // If at least one index can be useful, the union can be useful
    for (const auto & index_with_condition : indices_with_conditions)
    {
        if (index_with_condition.condition && !index_with_condition.condition->alwaysUnknownOrTrue())
            return false;
    }
    return true;
}

bool MergeTreeIndexUnionCondition::mayBeTrueOnGranule(const MergeTreeIndexGranules & granules) const
{
    // Call the version with empty part name for backward compatibility
    return mayBeTrueOnGranule(granules, "");
}

bool MergeTreeIndexUnionCondition::mayBeTrueOnGranule(const MergeTreeIndexGranules & granules, const String & part_name) const
{
    if (granules.size() != indices_with_conditions.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, 
            "Number of granules ({}) doesn't match number of indexes ({})", 
            granules.size(), indices_with_conditions.size());

    // Initialize statistics if needed
    if (collect_statistics && index_contributions.empty())
    {
        index_contributions.resize(indices_with_conditions.size());
        for (size_t i = 0; i < indices_with_conditions.size(); ++i)
        {
            index_contributions[i].index_name = indices_with_conditions[i].index->index.name;
        }
    }

    // Union logic: return true if ANY index says the granule may contain matching data
    bool any_passed = false;
    for (size_t i = 0; i < indices_with_conditions.size(); ++i)
    {
        const auto & index_with_condition = indices_with_conditions[i];
        if (!index_with_condition.condition)
            continue;

        bool passed = index_with_condition.condition->mayBeTrueOnGranule(granules[i]);
        
        if (collect_statistics)
        {
            index_contributions[i].granules_total++;
            if (passed)
            {
                index_contributions[i].granules_passed++;
                if (!part_name.empty())
                    index_contributions[i].parts_with_matches.insert(part_name);
                any_passed = true;
            }
        }
        else if (passed)
        {
            // Early return if not collecting statistics
            return true;
        }
    }

    // All indexes say the granule doesn't contain matching data
    return any_passed;
}

String MergeTreeIndexUnionCondition::getDescription() const
{
    std::vector<String> index_names;
    for (const auto & index_with_condition : indices_with_conditions)
    {
        index_names.push_back(index_with_condition.index->index.name);
    }
    
    // Manual join since fmt::join might not be available
    String joined_names;
    for (size_t i = 0; i < index_names.size(); ++i)
    {
        if (i > 0)
            joined_names += ", ";
        joined_names += index_names[i];
    }
    
    return fmt::format("Union of ({})", joined_names);
}

std::vector<String> MergeTreeIndexUnionCondition::getIndexNames() const
{
    std::vector<String> names;
    names.reserve(indices_with_conditions.size());
    for (const auto & index_with_condition : indices_with_conditions)
    {
        names.push_back(index_with_condition.index->index.name);
    }
    return names;
}

std::vector<MergeTreeIndexUnionCondition::IndexContribution> MergeTreeIndexUnionCondition::getIndexContributions() const
{
    return index_contributions;
}

} 
