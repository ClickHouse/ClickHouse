#include <optional>

#include <Storages/MergeTree/Compaction/PartsFinders/InMemoryPartsFinder.h>

namespace DB
{

static std::optional<MergeTreeData::DataPartsVector> findPartsInMemory(const MergeTreeData & data, const PartsRange & range)
{
    MergeTreeData::DataPartsVector data_parts;

    for (const auto & part : range)
    {
        /// FIXME: Outdated parts need to be fetched only inside transaction.
        auto data_part = data.getPartIfExists(part.part_info, {MergeTreeDataPartState::Active, MergeTreeDataPartState::Outdated});

        if (!data_part)
            return std::nullopt;
        else
            data_parts.push_back(std::move(data_part));
    }

    return data_parts;
}

InMemoryPartsFinder::InMemoryPartsFinder(const MergeTreeData & data_)
    : data(data_)
{
}

FutureMergedMutatedPartPtr InMemoryPartsFinder::constructFuturePart(const MergeSelectorChoice & choice) const
{
    auto data_parts = findPartsInMemory(data, choice.range);
    if (!data_parts.has_value())
        return nullptr;

    auto future_part = std::make_shared<FutureMergedMutatedPart>();
    future_part->merge_type = choice.merge_type;
    future_part->assign(std::move(data_parts.value()));

    return future_part;
}

}
