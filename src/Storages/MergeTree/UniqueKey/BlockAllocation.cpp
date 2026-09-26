#include <Storages/MergeTree/UniqueKey/BlockAllocation.h>

#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDeduplicationLog.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{

BlockAllocation::BlockAllocation(
    LoggerPtr log,
    std::unique_ptr<PlainCommittingBlockHolder> block_holder_,
    MergeTreeDeduplicationLog * deduplication_log_,
    IMergeTreeDataPart & part,
    const std::vector<DeduplicationHash> & deduplication_hashes)
    : block_holder(std::move(block_holder_))
    , deduplication_log(deduplication_log_)
{
    assignTo(part);
    part_info = part.info;

    if (deduplication_hashes.empty())
        return;

    chassert(deduplication_log);
    auto result = deduplication_log->addPart(getDeduplicationBlockIds(deduplication_hashes), part_info);
    for (const auto & res : result)
    {
        LOG_INFO(log, "Block with ID {} already exists as part {}; ignoring it",
            res.block_id, res.part_info.getPartNameForLogs());
        dedup_conflicts.push_back(res.block_id);
    }
    registered = dedup_conflicts.empty();
}

BlockAllocation::~BlockAllocation()
{
    if (!registered || committed)
        return;

    try
    {
        deduplication_log->dropPart(part_info);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void BlockAllocation::assignTo(IMergeTreeDataPart & part) const
{
    part.info.min_block = block_holder->block.number;
    part.info.max_block = block_holder->block.number;
    part.setName(part.getNewName(part.info));
}

}
