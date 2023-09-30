#include <Storages/MergeTree/InsertBlockInfo.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

AsyncInsertBlockInfo::AsyncInsertBlockInfo(
    Poco::Logger * log_,
    std::vector<std::string> && block_id_,
    BlockWithPartition && block_,
    std::optional<BlockWithPartition> && unmerged_block_with_partition_)
    : log(log_)
    , block_id(std::move(block_id_))
    , block_with_partition(std::move(block_))
    , unmerged_block_with_partition(std::move(unmerged_block_with_partition_))
{
    initBlockIDMap();
}

void AsyncInsertBlockInfo::initBlockIDMap()
{
    block_id_to_offset_idx.clear();
    for (size_t i = 0; i < block_id.size(); ++i)
    {
        block_id_to_offset_idx[block_id[i]].push_back(i);
    }
}

/// this function check if the block contains duplicate inserts.
/// if so, we keep only one insert for every duplicate ones.
bool AsyncInsertBlockInfo::filterSelfDuplicate()
{
    std::vector<String> dup_block_ids;
    for (const auto & [hash_id, offset_indexes] : block_id_to_offset_idx)
    {
        /// It means more than one inserts have the same hash id, in this case, we should keep only one of them.
        if (offset_indexes.size() > 1)
            dup_block_ids.push_back(hash_id);
    }
    if (dup_block_ids.empty())
        return false;

    filterBlockDuplicate(dup_block_ids, true);
    return true;
}

/// remove the conflict parts of block for rewriting again.
void AsyncInsertBlockInfo::filterBlockDuplicate(const std::vector<String> & block_paths, bool self_dedup)
{
    auto * current_block_with_partition = unmerged_block_with_partition.has_value() ? &unmerged_block_with_partition.value() : &block_with_partition;
    std::vector<size_t> offset_idx;
    for (const auto & raw_path : block_paths)
    {
        std::filesystem::path p(raw_path);
        String conflict_block_id = p.filename();
        auto it = block_id_to_offset_idx.find(conflict_block_id);
        if (it == block_id_to_offset_idx.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown conflict path {}", conflict_block_id);
        /// if this filter is for self_dedup, that means the block paths is selected by `filterSelfDuplicate`, which is a self purge.
        /// in this case, we don't know if zk has this insert, then we should keep one insert, to avoid missing this insert.
        offset_idx.insert(std::end(offset_idx), std::begin(it->second) + self_dedup, std::end(it->second));
    }
    std::sort(offset_idx.begin(), offset_idx.end());

    auto & offsets = current_block_with_partition->offsets;
    size_t idx = 0, remove_count = 0;
    auto it = offset_idx.begin();
    std::vector<size_t> new_offsets;
    std::vector<String> new_block_ids;

    /// construct filter
    size_t rows = current_block_with_partition->block.rows();
    auto filter_col = ColumnUInt8::create(rows, 1u);
    ColumnUInt8::Container & vec = filter_col->getData();
    UInt8 * pos = vec.data();
    for (auto & offset : offsets)
    {
        if (it != offset_idx.end() && *it == idx)
        {
            size_t start_pos = idx > 0 ? offsets[idx - 1] : 0;
            size_t end_pos = offset;
            remove_count += end_pos - start_pos;
            while (start_pos < end_pos)
            {
                *(pos + start_pos) = 0;
                start_pos++;
            }
            it++;
        }
        else
        {
            new_offsets.push_back(offset - remove_count);
            new_block_ids.push_back(block_id[idx]);
        }
        idx++;
    }

    LOG_TRACE(log, "New block IDs: {}, new offsets: {}, size: {}", toString(new_block_ids), toString(new_offsets), new_offsets.size());

    current_block_with_partition->offsets = std::move(new_offsets);
    block_id = std::move(new_block_ids);
    auto cols = current_block_with_partition->block.getColumns();
    for (auto & col : cols)
    {
        col = col->filter(vec, rows - remove_count);
    }
    current_block_with_partition->block.setColumns(cols);

    LOG_TRACE(log, "New block rows {}", current_block_with_partition->block.rows());

    initBlockIDMap();

    if (unmerged_block_with_partition.has_value())
        block_with_partition.block = unmerged_block_with_partition->block;
}

std::vector<String> AsyncInsertBlockInfo::getHashesForBlocks(BlockWithPartition & block, String partition_id)
{
    size_t start = 0;
    auto cols = block.block.getColumns();
    std::vector<String> block_id_vec;
    for (size_t i = 0; i < block.offsets.size(); ++i)
    {
        size_t offset = block.offsets[i];
        std::string_view token = block.tokens[i];
        if (token.empty())
        {
            SipHash hash;
            for (size_t j = start; j < offset; ++j)
            {
                for (const auto & col : cols)
                    col->updateHashWithValue(j, hash);
            }

            const auto hash_value = hash.get128();
            block_id_vec.push_back(partition_id + "_" + DB::toString(hash_value.items[0]) + "_" + DB::toString(hash_value.items[1]));
        }
        else
            block_id_vec.push_back(partition_id + "_" + std::string(token));

        start = offset;
    }
    return block_id_vec;
}

}
