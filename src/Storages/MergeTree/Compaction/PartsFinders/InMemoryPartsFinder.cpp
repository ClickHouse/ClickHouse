#include <Storages/MergeTree/Compaction/PartsFinders/InMemoryPartsFinder.h>

#include <optional>

namespace DB
{

static std::optional<MergeTreeDataPartsVector> findPartsInMemory(const MergeTreeData & data, const PartsRange & range, bool try_outdated)
{
    MergeTreeDataPartsVector data_parts;

    auto lookup_part = [&](const PartProperties & properties)
    {
        if (try_outdated)
            return data.getPartIfExists(properties.part_info, {MergeTreeDataPartState::Active, MergeTreeDataPartState::Outdated});
        else
            return data.getPartIfExists(properties.part_info, {MergeTreeDataPartState::Active});
    };

    for (const auto & properties : range)
    {
        if (auto part = lookup_part(properties))
            data_parts.push_back(std::move(part));
        else
            return std::nullopt;
    }

    return data_parts;
}

InMemoryPartsFinder::InMemoryPartsFinder(const MergeTreeData & data_, bool can_use_outdated_parts_)
    : data(data_)
    , can_use_outdated_parts(can_use_outdated_parts_)
{
}

FutureMergedMutatedPartPtr InMemoryPartsFinder::constructFuturePart(const MergeSelectorChoice & choice) const
{
    auto data_parts = findPartsInMemory(data, choice.range, can_use_outdated_parts);
    if (!data_parts.has_value())
        return nullptr;

    auto future_part = std::make_shared<FutureMergedMutatedPart>();
    future_part->merge_type = choice.merge_type;
    future_part->assign(std::move(data_parts.value()));

    return future_part;
}

}
