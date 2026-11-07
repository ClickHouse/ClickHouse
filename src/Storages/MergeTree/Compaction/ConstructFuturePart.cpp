#include <Storages/MergeTree/Compaction/ConstructFuturePart.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>

namespace DB
{

static std::expected<MergeTreeDataPartsVector, PreformattedMessage> findPartsInMemory(const MergeTreeData & data, const PartsRange & range, const MergeTreeData::DataPartStates & lookup_statuses)
{
    MergeTreeDataPartsVector data_parts;

    for (const auto & properties : range)
    {
        if (auto part = data.getPartIfExists(properties.info, lookup_statuses))
            data_parts.push_back(std::move(part));
        else
            return std::unexpected(PreformattedMessage::create("Part {} is not found", properties.name));
    }

    return data_parts;
}

std::expected<FutureMergedMutatedPartPtr, PreformattedMessage> constructFuturePart(
    const MergeTreeData & data,
    const MergeSelectorChoice & choice,
    MergeTreeData::DataPartStates lookup_statuses)
{
    auto parts = findPartsInMemory(data, choice.range, lookup_statuses);
    if (!parts)
        return std::unexpected(std::move(parts.error()));

    auto patch_parts = findPartsInMemory(data, choice.range_patches, lookup_statuses);
    if (!patch_parts)
        return std::unexpected(std::move(patch_parts.error()));

    auto future_part = std::make_shared<FutureMergedMutatedPart>();
    future_part->merge_type = choice.merge_type;
    if (choice.merge_type == MergeType::TTLClearIndex)
    {
        /// The result keeps the format of the source part.
        const auto & source_part = parts->front();
        const MergeTreeDataPartFormat source_format{source_part->getType(), source_part->getDataPartStorage().getType()};
        future_part->assign(std::move(*parts), std::move(*patch_parts), source_format);
    }
    else
        future_part->assign(std::move(*parts), std::move(*patch_parts), /*projection =*/nullptr);
    future_part->final = choice.final;

    return future_part;
}

}
