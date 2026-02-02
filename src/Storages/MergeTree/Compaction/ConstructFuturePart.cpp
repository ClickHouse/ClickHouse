#include <Storages/MergeTree/Compaction/ConstructFuturePart.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>

namespace DB
{

static std::optional<MergeTreeDataPartsVector> findPartsInMemory(const MergeTreeData & data, const PartsRange & range, MergeTreeData::DataPartStates lookup_statuses)
{
    MergeTreeDataPartsVector data_parts;

    for (const auto & properties : range)
    {
        if (auto part = data.getPartIfExists(properties.info, lookup_statuses))
            data_parts.push_back(std::move(part));
        else
            return std::nullopt;
    }

    return data_parts;
}

FutureMergedMutatedPartPtr constructFuturePart(const MergeTreeData & data, const MergeSelectorChoice & choice, MergeTreeData::DataPartStates lookup_statuses)
{
    auto data_parts = findPartsInMemory(data, choice.range, std::move(lookup_statuses));
    if (!data_parts.has_value())
        return nullptr;

    auto future_part = std::make_shared<FutureMergedMutatedPart>();
    future_part->merge_type = choice.merge_type;
    future_part->assign(std::move(data_parts.value()));
    future_part->final = choice.final;

    return future_part;
}

}
