#include <Processors/Transforms/DeduplicationTokenTransforms.h>
#include <Interpreters/InsertDeduplication.h>
#include <Interpreters/InsertDependenciesBuilder.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Common/Logger.h>
#include <Common/ErrorCodes.h>
#include <Common/logger_useful.h>
#include <Core/SettingsEnums.h>
#include <fmt/ranges.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
};


RestoreChunkInfosTransform::RestoreChunkInfosTransform(Chunk::ChunkInfoCollection chunk_infos_, SharedHeader header_)
    : ISimpleTransform(header_, header_, true)
    , chunk_infos(std::move(chunk_infos_))
{
}


void RestoreChunkInfosTransform::transform(Chunk & chunk)
{
    if (auto info = chunk.getChunkInfos().get<DeduplicationInfo>())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Chunk already has DeduplicationInfo when restoring chunk infos, existing deduplication info debug: {}, restoring chunk infos: {}",
            info->debug(),
            chunk_infos.debug());
    }

    auto old_infos = std::move(chunk.getChunkInfos());
    chunk.setChunkInfos(chunk_infos.clone());
    chunk.getChunkInfos().appendIfUniq(std::move(old_infos));

    LOG_TEST(getLogger("RestoreChunks"), "Restoring chunk infos, result: {}",
        chunk.getChunkInfos().debug());
}


UpdateDeduplicationInfoWithViewIDTransform::UpdateDeduplicationInfoWithViewIDTransform(StorageIDMaybeEmpty view_id_, SharedHeader header_)
    : ISimpleTransform(header_, header_, true)
    , view_id(std::move(view_id_))
{
}


void UpdateDeduplicationInfoWithViewIDTransform::transform(Chunk & chunk)
{
    auto info = chunk.getChunkInfos().getSafe<DeduplicationInfo>();
    info->setViewID(view_id);
    info->setViewBlockNumber(block_number++);
}


SelectPartitionTransform::SelectPartitionTransform(std::string partition_id_, StorageMetadataPtr metadata_snapshot_, ContextPtr contex_, SharedHeader header_)
    : ISimpleTransform(header_, header_, true)
    , partition_id(std::move(partition_id_))
    , metadata_snapshot(std::move(metadata_snapshot_))
    , context(std::move(contex_))
{
}


void SelectPartitionTransform::transform(Chunk & chunk)
{
    auto rows_in_source_chunk = chunk.getNumRows();

    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    size_t max_parts = 0; // do not limit here part count
    BlocksWithPartition part_blocks = MergeTreeDataWriter::splitBlockIntoParts(std::move(block), max_parts, metadata_snapshot, context);

    std::vector<std::string> all_partitions;
    for (const auto & current_block : part_blocks)
        all_partitions.push_back(current_block.partition_id + "size=" + DB::toString(current_block.block->rows()));

    Chunk result_chunk;
    for (auto & current_block : part_blocks)
    {
        if (current_block.partition_id == partition_id)
        {
            result_chunk = Chunk(current_block.block->getColumns(), current_block.block->rows());
            result_chunk.setChunkInfos(std::move(chunk.getChunkInfos()));
            break;
        }
    }

    LOG_DEBUG(getLogger("Deduplication::SelectPartitionTransform"),
        "Selecting partition '{}' with {} rows from chunk with rows {}, total partitions in chunk: {}, partitions: {}",
        partition_id,
        result_chunk.getNumRows(),
        rows_in_source_chunk,
        all_partitions.size(),
        fmt::join(all_partitions, ","));

    result_chunk.getChunkInfos().appendIfUniq(std::move(chunk.getChunkInfos()));

    chunk = std::move(result_chunk);
}


AddDeduplicationInfoTransform::AddDeduplicationInfoTransform(
    SharedHeader header_)
    : ISimpleTransform(header_, header_, true)
{
}


AddDeduplicationInfoTransform::AddDeduplicationInfoTransform(
    InsertDependenciesBuilderConstPtr insert_dependencies_,
    StorageIDMaybeEmpty root_view_id_,
    std::string user_token_,
    InsertDeduplicationVersions unification_stage_,
    SharedHeader header_)
    : ISimpleTransform(header_, header_, true)
    , insert_dependencies(std::move(insert_dependencies_))
    , root_view_id(std::move(root_view_id_))
    , user_token(std::move(user_token_))
    , unification_stage(unification_stage_)
{
}


void AddDeduplicationInfoTransform::transform(Chunk & chunk)
{
    if (!chunk.getChunkInfos().has<DeduplicationInfo>())
    {
        auto info = DeduplicationInfo::create(false, unification_stage);
        info->setUserToken(user_token, chunk.getNumRows());
        chunk.getChunkInfos().add(info);
    }

    auto info = chunk.getChunkInfos().getSafe<DeduplicationInfo>();
    info->setInsertDependencies(insert_dependencies);
    info->setRootViewID(root_view_id);
    info->setSourceBlockNumber(block_number++);
    info->updateOriginalBlock(chunk, getInputPort().getSharedHeader());
}

RedefineDeduplicationInfoWithDataHashTransform::RedefineDeduplicationInfoWithDataHashTransform(SharedHeader header_)
    : ISimpleTransform(header_, header_, true)
{
}

void RedefineDeduplicationInfoWithDataHashTransform::transform(Chunk & chunk)
{
    auto info = chunk.getChunkInfos().getSafe<DeduplicationInfo>();
    info->redefineTokensWithDataHash(getOutputPort().getSharedHeader()->cloneWithColumns(chunk.getColumns()));
}
}
