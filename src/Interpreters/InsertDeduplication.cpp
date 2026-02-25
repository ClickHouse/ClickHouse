#include <deque>
#include <filesystem>
#include <memory>
#include <vector>
#include <Interpreters/InsertDeduplication.h>
#include <Interpreters/InsertDependenciesBuilder.h>
#include <Interpreters/Context.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Chunk.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Columns/IColumn.h>
#include <Core/Settings.h>
#include <Core/Block.h>
#include <IO/WriteHelpers.h>
#include <Common/PODArray.h>
#include <Common/ErrorCodes.h>
#include <Common/SipHash.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <Interpreters/StorageIDMaybeEmpty.h>

#include <base/defines.h>
#include <base/scope_guard.h>

#include <fmt/format.h>
#include <fmt/ranges.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}


namespace Setting
{
    extern const SettingsBool use_async_executor_for_materialized_views;
}


DeduplicationHash::DeduplicationHash(UInt128 hash_, std::string partition_id_, HashType htype)
    : hash(std::move(hash_))
    , partition_id(std::move(partition_id_))
    , hash_type(htype)
{}


DeduplicationHash DeduplicationHash::createUnifiedHash(UInt128 hash, std::string partition_id)
{
    return DeduplicationHash(hash, std::move(partition_id), HashType::UNIFIED);
}


DeduplicationHash DeduplicationHash::createSyncHash(UInt128 hash, std::string partition_id)
{
    return DeduplicationHash(hash, std::move(partition_id), HashType::SYNC);
}


DeduplicationHash DeduplicationHash::createAsyncHash(UInt128 hash, std::string partition_id)
{
    return DeduplicationHash(hash, std::move(partition_id), HashType::ASYNC);
}


std::string DeduplicationHash::getBlockId() const
{
    return fmt::format("{}_{}_{}", partition_id, hash.items[0], hash.items[1]);
}


std::string DeduplicationHash::getPath(const std::string & storage_path) const
{
    std::string hashes_directory = [&] ()
    {
        switch (hash_type)
        {
            case HashType::SYNC:
                return "blocks";
            case HashType::ASYNC:
                return "async_blocks";
            case HashType::UNIFIED:
                return "deduplication_hashes";
        }
    }();
    return (std::filesystem::path(storage_path) / hashes_directory / getBlockId()).string();
}


void DeduplicationHash::setConflictPartName(const std::string & part_name)
{
    conflicted_part_name = part_name;
}


bool DeduplicationHash::hasConflictPartName() const
{
    return conflicted_part_name.has_value();
}


std::string DeduplicationHash::getConflictPartName() const
{
    chassert(conflicted_part_name.has_value());
    return conflicted_part_name.value_or("");
}


std::vector<std::string> getDeduplicationBlockIds(const std::vector<DeduplicationHash> & deduplication_hashes)
{
    std::vector<std::string> result;
    result.reserve(deduplication_hashes.size());
    for (const auto & deduplication_hash : deduplication_hashes)
    {
        result.push_back(deduplication_hash.getBlockId());
    }
    return result;
}


std::vector<std::string> getDeduplicationPaths(std::string storage_path, const std::vector<DeduplicationHash> & deduplication_hashes)
{
    std::vector<std::string> result;
    result.reserve(deduplication_hashes.size());
    for (const auto & deduplication_hash : deduplication_hashes)
    {
        result.push_back(deduplication_hash.getPath(storage_path));
    }
    return result;
}


static std::atomic<size_t> deduplication_info_id_counter{0};


DeduplicationInfo::FilterResult DeduplicationInfo::deduplicateSelf(bool deduplication_enabled, const std::string & partition_id, ContextPtr context) const
{
    if (disabled || !deduplication_enabled)
    {
        return {};
    }

    return recalculateBlock(filterImpl(filterSelf(partition_id)), partition_id, context);
}


DeduplicationInfo::FilterResult DeduplicationInfo::recalculateBlock(DeduplicationInfo::FilterResult && filtered, const std::string & partition_id, ContextPtr context) const
{
    if (filtered.removed_rows == 0)
        return {};

    auto chunk = Chunk(filtered.filtered_block->getColumns(), filtered.filtered_block->rows());

    auto token_for_retry = filtered.deduplication_info->cloneSelf();
    token_for_retry->truncateTokensForRetry();
    LOG_DEBUG(logger, "token to retry with: {}", token_for_retry->debug());
    chunk.getChunkInfos().add(std::move(token_for_retry));

    auto header = std::make_shared<const Block>(filtered.filtered_block->cloneEmpty());

    auto block = goRetry(std::move(header), std::move(chunk), filtered.deduplication_info, partition_id, context);
    filtered.filtered_block = std::make_shared<Block>(std::move(block));
    return std::move(filtered);
}


std::set<size_t> DeduplicationInfo::filterSelf(const String & partition_id) const
{
    if (getCount() <= 1)
        return {};

    auto block_id_to_offsets = buildBlockIdToOffsetsMap(partition_id);

    std::set<size_t> fitered_offsets;
    /// fitered_offsets will contain all but first offsets for each block id
    /// so that only first occurrence of each block id will remain
    for (auto & [_, block_offsets] : block_id_to_offsets)
    {
        if (block_offsets.size() > 1)
            fitered_offsets.insert(block_offsets.begin() + 1, block_offsets.end());
    }

    return fitered_offsets;
}


std::set<size_t> DeduplicationInfo::filterOriginal(const std::vector<std::string> & collisions, const String & partition_id) const
{
    chassert(getCount() == 1 || (original_block && !original_block->empty()));

    if (collisions.empty())
        return {};

    auto block_id_to_offsets = buildBlockIdToOffsetsMap(partition_id);

    std::set<size_t> fitered_offsets;
    for (const auto & collision :collisions)
        if (auto it = block_id_to_offsets.find(collision); it != block_id_to_offsets.end())
            fitered_offsets.insert(it->second.begin(), it->second.end());

    return fitered_offsets;
}


DeduplicationInfo::Ptr DeduplicationInfo::cloneSelfFilterImpl() const
{
    LOG_TEST(logger, "Cloning deduplication info for filtering, debug: {}", debug());
    auto new_instance = DeduplicationInfo::create(is_async_insert, unification_stage);
    new_instance->disabled = disabled;
    new_instance->level = level;
    new_instance->visited_views = visited_views;
    new_instance->insert_dependencies = insert_dependencies;
    new_instance->retried_view_id = retried_view_id;
    new_instance->original_block_view_id = original_block_view_id;
    return new_instance;
}

DeduplicationInfo::Ptr DeduplicationInfo::cloneMergeImpl() const
{
    LOG_TEST(logger, "Cloning deduplication info for merging, debug: {}", debug());
    auto new_instance = DeduplicationInfo::create(is_async_insert, unification_stage);
    new_instance->disabled = disabled;
    new_instance->level = level;
    new_instance->visited_views = visited_views;
    new_instance->insert_dependencies = insert_dependencies;
    new_instance->retried_view_id = retried_view_id;
    return new_instance;
}


DeduplicationInfo::FilterResult DeduplicationInfo::filterImpl(const std::set<size_t> & collision_offsets) const
{
    if (collision_offsets.empty())
        return {};

    if (!is_async_insert && getCount() == 1)
    {
        chassert(collision_offsets.size() == 1 && collision_offsets.contains(0));
        LOG_TEST(logger, "The only token is filtered, collision offsets: {}, debug: {}", fmt::join(collision_offsets, ", "), debug());

        Ptr new_tokens = cloneSelfFilterImpl();
        new_tokens->original_block = std::make_shared<Block>(original_block->cloneEmpty());

        return {
            .filtered_block = new_tokens->original_block,
            .deduplication_info = new_tokens,
            .removed_rows = getTokenRows(0),
            .removed_tokens = 1,
        };
    }

    chassert(original_block && !original_block->empty() && original_block->rows() > 0);

    auto & block = *original_block;

    Ptr new_tokens = cloneSelfFilterImpl();

    size_t removed_rows = 0;
    size_t removed_tokens = 0;
    PaddedPODArray<UInt8> filer_column;
    filer_column.resize_fill(block.rows(), 1);

    for (size_t i = 0; i < offsets.size(); ++i)
    {
        if (collision_offsets.contains(i))
        {
            removed_rows += getTokenEnd(i) - getTokenBegin(i);
            removed_tokens += 1;
            for (auto row_id = getTokenBegin(i); row_id < getTokenEnd(i); ++row_id)
                filer_column[row_id] = 0;
        }
        else
        {
            new_tokens->tokens.push_back(tokens[i]);
            new_tokens->offsets.push_back(new_tokens->getRows() + getTokenRows(i));
        }
    }

    chassert(removed_rows > 0);
    chassert(removed_rows <= block.rows());

    if (removed_rows == block.rows())
    {
        chassert(removed_tokens == getCount());
        new_tokens->original_block = std::make_shared<Block>(block.cloneEmpty());

        LOG_TEST(
            logger,
            "All {} rows are removed due to duplicate, debug: {}",
            block.rows(),
            debug());

        return {
            .filtered_block = new_tokens->original_block,
            .deduplication_info = new_tokens,
            .removed_rows = removed_rows,
            .removed_tokens = removed_tokens
        };
    }

    auto cols = block.getColumns();
    for (auto & col : cols)
        col = col->filter(filer_column, block.rows() - removed_rows);

    Block filtered_block = block.cloneWithoutColumns();
    filtered_block.setColumns(cols);

    chassert(filtered_block.rows() == new_tokens->getRows());
    chassert(filtered_block.rows() + removed_rows == block.rows());

    new_tokens->original_block = std::make_shared<Block>(std::move(filtered_block));

    LOG_DEBUG(
        logger,
        "Filtered {}/{} rows with {}/{} tokens as duplicates, remaining rows/count={}/{}, debug: {}",
        removed_rows,
        block.rows(),
        collision_offsets.size(),
        getCount(),
        new_tokens->original_block->rows(),
        new_tokens->getCount(),
        debug());

    return {
        .filtered_block = new_tokens->original_block,
        .deduplication_info = new_tokens,
        .removed_rows = removed_rows,
        .removed_tokens = removed_tokens
    };
}


UInt128 DeduplicationInfo::calculateDataHash(size_t offset, const Block & block) const
{
    chassert(offset < offsets.size());

    if (tokens[offset].data_hash.has_value())
        return tokens[offset].data_hash.value();

    chassert(block.rows() == getRows());

    auto cols = block.getColumns();

    SipHash hash;
    for (size_t j = getTokenBegin(offset); j < getTokenEnd(offset); ++j)
    {
        for (const auto & col : cols)
            col->updateHashWithValue(j, hash);
    }

    /// be careful, hash.get128() method is not const because of caching of calculated hash in token, so it can return different results on multiple calls
    tokens[offset].data_hash = hash.get128();
    return tokens[offset].data_hash.value();
}


DeduplicationHash DeduplicationInfo::getBlockUnifiedHash(size_t offset, const std::string & partition_id) const
{
    // do not take into account source token.by_data from part writer, calculate full hash of data
    // this hash would be used for deduplication within sync and async inserts in unified manner

    auto & token = tokens[offset];

    std::string extension;
    if (!token.by_user.empty())
    {
        extension = "user-token-" + token.by_user;
    }
    else
    {
        auto data_hash = calculateDataHash(offset, *original_block);
        extension = fmt::format("{}_{}", data_hash.items[0], data_hash.items[1]);
    }

    // for other token sources addition parts are appended
    for (const auto & extra : token.extra_tokens)
    {
        if (is_async_insert && extra.type == TokenDefinition::Extra::SOURCE_NUMBER)
        {
            // do not include source number for async inserts, they are not relevant as data hash is used or user token
            // a token describes only the data in one block
            extension.append(":");
            extension.append(TokenDefinition::Extra::asSourceNumber(0).toString());
            continue;
        }

        extension.append(":");
        extension.append(extra.toString());
    }

    LOG_TEST(logger, "getBlockUnifiedHash {} debug: {}", extension, debug());

    SipHash hash;
    hash.update(extension.data(), extension.size());
    return DeduplicationHash::createUnifiedHash(hash.get128(), partition_id);
}


DeduplicationHash DeduplicationInfo::getBlockHash(size_t offset, const std::string & partition_id) const
{
    // if user token is empty we calculate by_data_hash
    auto & token = tokens[offset];
    if (token.empty())
    {
        chassert(level == Level::SOURCE);
        token.by_part_writer = calculateDataHash(offset, *original_block);
    }

    if (token.by_part_writer.has_value() && level == Level::SOURCE)
    {
        if (is_async_insert)
            return DeduplicationHash::createAsyncHash(token.by_part_writer.value(), partition_id);
        else
            return DeduplicationHash::createSyncHash(token.by_part_writer.value(), partition_id);
    }

    // only one value is set here
    std::string extension;

    if (!token.by_user.empty())
        extension = "user-token-" + token.by_user;
    else
        extension = fmt::format("{}_{}", token.by_part_writer->items[0], token.by_part_writer->items[1]);

    // for other token sources addition parts are appended
    for (const auto & extra : token.extra_tokens)
    {
        if (extra.type == TokenDefinition::Extra::SOURCE_ID)
            continue; // do not include source id

        if (token.by_part_writer.has_value()
            && (extra.type == TokenDefinition::Extra::SOURCE_ID || extra.type == TokenDefinition::Extra::SOURCE_NUMBER))
            continue; // source id is already included in by_data

        extension.append(":");
        if (is_async_insert && extra.type == TokenDefinition::Extra::SOURCE_NUMBER)
        {
            // do not include source number for async inserts,
            // they are not relevant as data hash is used or user token
            // a token describes only the data in one block
            extension.append(TokenDefinition::Extra::asSourceNumber(0).toString());
        }
        else
            extension.append(extra.toString());
    }

    LOG_TEST(logger, "getBlockHash {} debug: {}", extension, debug());

    SipHash hash;
    hash.update(extension.data(), extension.size());
    if (is_async_insert)
        return DeduplicationHash::createAsyncHash(hash.get128(), partition_id);
    else
        return DeduplicationHash::createSyncHash(hash.get128(), partition_id);
}


std::unordered_map<std::string, std::vector<size_t>> DeduplicationInfo::buildBlockIdToOffsetsMap(const std::string & partition_id) const
{
    std::unordered_map<std::string, std::vector<size_t>> result;

    for (size_t offset = 0; offset < offsets.size(); ++offset)
    {
        for (auto & block_hash : chooseDeduplicationHashes(offset, partition_id))
            result[block_hash.getBlockId()].push_back(offset);
    }

    return result;
}


std::vector<DeduplicationHash> DeduplicationInfo::chooseDeduplicationHashes(size_t offset, const std::string & partition_id) const
{
    std::vector<DeduplicationHash> result;
    switch (unification_stage)
    {
        case InsertDeduplicationVersions::OLD_SEPARATE_HASHES:
            result.push_back(getBlockHash(offset, partition_id));
            break;
        case InsertDeduplicationVersions::COMPATIBLE_DOUBLE_HASHES:
            result.push_back(getBlockHash(offset, partition_id));
            result.push_back(getBlockUnifiedHash(offset, partition_id));
            break;
        case InsertDeduplicationVersions::NEW_UNIFIED_HASHES:
            result.push_back(getBlockUnifiedHash(offset, partition_id));
            break;
    }
    return result;
}


std::vector<DeduplicationHash> DeduplicationInfo::getDeduplicationHashes(const std::string & partition_id, bool deduplication_enabled) const
{
    LOG_TEST(logger, "getDeduplicationHashes for partition_id={}, deduplication_enabled: {}, debug: {}", partition_id, deduplication_enabled, debug());
    if (disabled || !deduplication_enabled)
        return {};

    std::vector<DeduplicationHash> result;
    result.reserve(2*offsets.size());

    for (size_t offset = 0; offset < offsets.size(); ++offset)
    {
        for (auto & block_hash : chooseDeduplicationHashes(offset, partition_id))
            result.push_back(std::move(block_hash));
    }

    return result;
}


size_t DeduplicationInfo::getCount() const
{
    return offsets.size();
}


size_t DeduplicationInfo::getRows() const
{
    if (offsets.empty())
        return 0;
    return offsets.back();
}


std::pair<std::string, size_t> DeduplicationInfo::debug(size_t offset) const
{
    chassert(offset < offsets.size());
    const auto & token = tokens[offset];
    if (token.empty())
        return {"-", getTokenEnd(offset)};
    else if (!token.by_user.empty())
        return {tokens[offset].by_user, getTokenEnd(offset)};
    else
        return {fmt::format("{}_{}", token.by_part_writer->items[0], token.by_part_writer->items[1]), getTokenEnd(offset)};
}


std::string DeduplicationInfo::debug() const
{
    std::vector<std::string> token_strs;
    for (size_t i = 0; i < tokens.size(); ++i)
    {
        const auto & token = tokens[i];
        token_strs.push_back(token.debug());
        token_strs.back() += fmt::format("::{}", getTokenEnd(i));
    }

    if (token_strs.size() > 10)
    {
        token_strs.resize(10);
        token_strs.push_back("...");
    }

    std::string block_str;
    if (!original_block)
        block_str = "null";
    else if (original_block->empty())
        block_str = "empty";
    else
        block_str = fmt::format("rows/cols {}/{}", original_block->rows(), original_block->getColumns().size());

    std::vector<std::string> data_hashes;
    for (const auto & token : tokens)
    {
        if (token.data_hash.has_value())
            data_hashes.push_back(fmt::format("{}_{}", token.data_hash->items[0], token.data_hash->items[1]));
        else
            data_hashes.push_back("-");
    }

    return fmt::format(
        "instance_id: {}, {}, {}, level {}, rows/tokens {}/{}, in block: {}, tokens: {}:[{}], visited views: {}:[{}], retried view id: {}, original block id: {}, data_hashes: {}, unification_stage {}",
        instance_id,
        is_async_insert ? "async" : "sync",
        disabled ? "disabled" : "enabled",
        level,
        getRows(), getCount(),
        block_str,
        getCount(), fmt::join(token_strs, ","),
        visited_views.size(), fmt::join(visited_views, ","),
        retried_view_id,
        original_block_view_id,
        fmt::join(data_hashes, ","),
        unification_stage);
}


DeduplicationInfo::Ptr DeduplicationInfo::create(bool async_insert_, InsertDeduplicationVersions unification_stage_)
{
    struct make_shared_enabler : public DeduplicationInfo
    {
        make_shared_enabler(bool async_insert_, InsertDeduplicationVersions unification_stage_)
            : DeduplicationInfo(async_insert_, unification_stage_)
        {}
    };
    return std::make_shared<make_shared_enabler>(async_insert_, unification_stage_);
}


DeduplicationInfo::Ptr DeduplicationInfo::cloneSelf() const
{
    return std::make_shared<DeduplicationInfo>(*this);
}


ChunkInfo::Ptr DeduplicationInfo::clone() const
{
    return std::static_pointer_cast<ChunkInfo>(cloneSelf());
}


void DeduplicationInfo::setPartWriterHashForPartition(UInt128 hash, size_t count) const
{
    LOG_TEST(
        logger,
        "setPartWriterHashForPartition: hash={}_{} count={}, debug: {}",
        hash.items[0],
        hash.items[1],
        count,
        debug());

    if (disabled)
        return;

    if (level != Level::SOURCE)
        return;

    if (is_async_insert)
        return;

    chassert(getCount() >= 1);

    if (getCount() > 1)
        return;

    if (!tokens[0].empty())
        return;

    tokens[0].setDataToken(hash);
}


void DeduplicationInfo::setPartWriterHashes(const std::vector<UInt128> & partitions_hashes, size_t count) const
{
    LOG_TEST(
        logger,
        "setPartWriterHashes: tokens='{}' count={}, debug: {}",
        partitions_hashes.size(),
        count,
        debug());

    // if (disabled)
    //     return;

    if (is_async_insert)
        return;

    if (level != Level::SOURCE)
        return;

    chassert(getCount() >= 1);

    if (getCount() > 1)
        return;

    if (!tokens[0].empty())
        return;

    if (partitions_hashes.size() != 1)
    {
        /// we can set only one hash here
        /// if there are multiple partitions in chunk then data hash would be calculated later
        /// by hash of the whole chunk
        return;
    }

    tokens[0].setDataToken(partitions_hashes[0]);

    chassert(getRows() == count);
}

/// It is to define data hash for the chunk if it was not defined before by user token or part writer token
/// that happens in the case when target table has storage null and dependent views have storage with non-null,
/// so we cannot use part writer token as user token for dependent views, we have to calculate data hash
void DeduplicationInfo::redefineTokensWithDataHash(const Block & block)
{
    LOG_TEST(logger, "redefineTokensWithDataHash, debug: {}", debug());

    if (disabled || level != Level::SOURCE)
        return;

    chassert(original_block);

    if (!is_async_insert && getCount() == 1)
    {
        chassert(original_block->rows() == 0);
        /// we have optimized case for one token, empty block are stored in original_block
        /// but we have columns in the chunk to calculate hash, so we can calculate data hash for the token if it is not set before
        if (tokens[0].empty())
        {
            // when migration has been started, data_hash is set in `updateOriginalBlock` method
            chassert(unification_stage == InsertDeduplicationVersions::OLD_SEPARATE_HASHES || tokens[0].data_hash.has_value());
            [[maybe_unused]] auto unused = calculateDataHash(0, block);
        }
    }

    for (size_t i = 0; i < tokens.size(); ++i)
    {
        auto & token = tokens[i];
        if (token.empty())
        {
            /// calculate tokens from data
            token.by_part_writer = calculateDataHash(i, *original_block);
        }
    }
}


DeduplicationInfo::DeduplicationInfo(bool async_insert_, InsertDeduplicationVersions unification_stage_)
    : instance_id(deduplication_info_id_counter.fetch_add(1, std::memory_order_relaxed))
    , is_async_insert(async_insert_)
    , unification_stage(unification_stage_)
{
    LOG_TEST(logger, "Create DeduplicationInfo, debug: {}", debug());
}


DeduplicationInfo::DeduplicationInfo(const DeduplicationInfo & other)
    : ChunkInfo(other)
    , instance_id(deduplication_info_id_counter.fetch_add(1, std::memory_order_relaxed))
    , is_async_insert(other.is_async_insert)
    , unification_stage(other.unification_stage)
    , insert_dependencies(other.insert_dependencies)
    , disabled(other.disabled)
    , level(other.level)
    , tokens(other.tokens)
    , offsets(other.offsets)
    , original_block(other.original_block)
    , original_block_view_id(other.original_block_view_id)
    , visited_views(other.visited_views)
    , retried_view_id(other.retried_view_id)
{
    LOG_TEST(logger, "Clone DeduplicationInfo {} from {}", instance_id, other.debug());
}


void DeduplicationInfo::setUserToken(const String & token, size_t count)
{
    chassert(level == Level::SOURCE);

    if (count == 0)
        return;

    tokens.push_back(TokenDefinition::asUserToken(token));
    offsets.push_back(getRows() + count);

    LOG_TEST(logger, "setUserToken: token={} count={}, {}", token, count, debug());
}


DeduplicationInfo::TokenDefinition DeduplicationInfo::TokenDefinition::asUserToken(std::string token)
{
    TokenDefinition t;
    t.by_user = std::move(token);
    return t;
}


void DeduplicationInfo::TokenDefinition::setDataToken(UInt128 token)
{
    if (!empty())
        return;
    by_part_writer = token;
}


namespace
{
using FilterPredicate = std::function<bool (const Chunk &)>;

template <typename Executor>
std::deque<Chunk> exec(QueryPipeline & pipeline, FilterPredicate predicate)
{
    std::deque<Chunk> result;

    auto executor = Executor(pipeline);
    bool is_done = false;
    while (!is_done)
    {
        Chunk chunk;
        is_done = !executor.pull(chunk);

        if (!chunk)
            continue;

        if (!predicate(chunk))
            continue;

        result.push_back(std::move(chunk));
    }

    return result;
}
}


void DeduplicationInfo::truncateTokensForRetry()
{
    // we keep only source or view id and source or view number extra tokens which is pointed by original_block_view_id
    // all other extra tokens are removed

    for (auto & token : tokens)
    {
        auto it = std::find_if(
            token.extra_tokens.begin(),
            token.extra_tokens.end(),
            [&] (const TokenDefinition::Extra & extra)
            {
                if (extra.type == TokenDefinition::Extra::Type::SOURCE_ID || extra.type == TokenDefinition::Extra::Type::VIEW_ID)
                    return std::get<StorageIDMaybeEmpty>(extra.value_variant) == original_block_view_id;
                return false;
            });

        if (it == token.extra_tokens.end())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Invalid deduplication token structure during retry, missing source/view id {} for original block view. id, debug: {}",
                original_block_view_id,
                debug());

        auto next = ++it;
        if (next == token.extra_tokens.end() || (next->type != TokenDefinition::Extra::Type::SOURCE_NUMBER && next->type != TokenDefinition::Extra::Type::VIEW_NUMBER))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Invalid deduplication token structure during retry, expected source/view number after source/view id, debug: {}",
                debug());
        ++next;
        token.extra_tokens.erase(next, token.extra_tokens.end());
    }

    retried_view_id = visited_views.back();
}


Block DeduplicationInfo::goRetry(SharedHeader && header, Chunk && filtered_data, Ptr filtered_info, const std::string & partition_id, ContextPtr context) const
{
    bool is_empty = !filtered_data || filtered_data.getNumRows() == 0;

    auto builder = QueryPipelineBuilder();
    builder.init(Pipe(std::make_shared<SourceFromSingleChunk>(std::move(header), std::move(filtered_data))));
    builder.addChain(insert_dependencies->createChainForDeduplicationRetry(*this, partition_id));

    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));
    chassert(pipeline.pulling());

    auto result_header = pipeline.getSharedHeader();

    // in case all rows are filtered out
    // we should not run the pipeline
    // because no data no results
    // otherwise we can end up in a cycle when all data is filtered by inner query return not empty aggregate result
    if (is_empty)
        return result_header->cloneEmpty();

    auto filter =[this, filtered_info] (const Chunk & chunk) -> bool
    {
        auto info = chunk.getChunkInfos().get<DeduplicationInfo>();

        if (!chunk)
        {
            LOG_TEST(this->logger, "skip empty chunk with deduplication info: {}", info ? info->debug() : "null");
            return false;
        }

        if (!info)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Chunk has no deduplication info during deduplication retry, expected deduplication info debug: {}",
                filtered_info->debug());

        if (info->tokens != filtered_info->tokens)
        {
            LOG_TEST(this->logger, "skip chunk with deduplication info: {}", info->debug());
            return false;
        }

        return true;
    };

    auto result_chunks = context->getSettingsRef()[Setting::use_async_executor_for_materialized_views]
        ? exec<PullingAsyncPipelineExecutor>(pipeline, std::move(filter))
        : exec<PullingPipelineExecutor>(pipeline, std::move(filter));

    if (result_chunks.size() > 1)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Multiple chunks returned during deduplication retry, expected single chunk, chunks {}, deduplication info debug: {}",
            result_chunks.size(),
            filtered_info->debug());
        }

    if (result_chunks.empty())
        return result_header->cloneEmpty();

    return result_header->cloneWithColumns(result_chunks[0].detachColumns());
}


void DeduplicationInfo::updateOriginalBlock(const Chunk & chunk, SharedHeader header)
{
    chassert(!visited_views.empty());

    if (!original_block)
        original_block_view_id = visited_views.back();

    if (disabled)
    {
        chassert(!original_block);
        return;
    }

    if (!is_async_insert && getCount() == 1)
    {
        /// In this case we can omit original block rows to save memory
        /// if there is a duplicate is found in the original block then we tottaly filter out all rows in the block and original block will be not used at all

        /// but we still need the original blocks data hash, lets calculate it here when we have all information about the block,
        /// so we can use it for deduplication later in the pipeline

        if (unification_stage != InsertDeduplicationVersions::OLD_SEPARATE_HASHES)
        {
            auto block = header->cloneWithColumns(chunk.getColumns());
            /// it is enough to call calculateDataHash for one of tokens, the hash would be saved for this token in `data_hash` field and used later for deduplication
            [[maybe_unused]] auto unused = calculateDataHash(0, block);
            LOG_TEST(
                logger,
                "Calculated data hash for the original block with cols/rows: {}/{} in updateOriginalBlock and omit the original block, debug: {}",
                block.columns(),
                block.rows(),
                debug());
        }

        // still we still need the header of the original block for correct work of some functions like filter
        original_block = std::make_shared<Block>(header->cloneEmpty());

        return;
    }

    original_block = std::make_shared<Block>(header->cloneWithColumns(chunk.getColumns()));

}


void DeduplicationInfo::setInsertDependencies(InsertDependenciesBuilderConstPtr insert_dependencies_)
{
    insert_dependencies = std::move(insert_dependencies_);
}


void DeduplicationInfo::setRootViewID(const StorageIDMaybeEmpty & id)
{
    LOG_TEST(logger, "Setting root view ID '{}' in deduplication tokens", id);
    chassert(level == Level::SOURCE);

    if (!insert_dependencies || !insert_dependencies->deduplicate_blocks)
        disabled = true;

    addExtraPart(TokenDefinition::Extra::asSourceID(id));
    visited_views.push_back(id);
}


void DeduplicationInfo::setViewID(const StorageID & id)
{
    LOG_TEST(logger, "Setting view ID '{}', debug: {}", id, debug());

    if (level == Level::SOURCE)
        level = Level::VIEW;

    chassert(level == Level::VIEW);

    if (!insert_dependencies || !insert_dependencies->deduplicate_blocks_in_dependent_materialized_views)
    {
        disabled = true;
        original_block.reset(); // do not hold original block if deduplication is disabled, to save memory
    }

    addExtraPart(TokenDefinition::Extra::asViewID(id));
    visited_views.push_back(id);
}


void DeduplicationInfo::setViewBlockNumber(size_t block_number)
{
    chassert(level == Level::VIEW);
    addExtraPart(TokenDefinition::Extra::asViewNumber(block_number));

    if (!is_async_insert || disabled)
        return;

    if (block_number > 0)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "The deduplication with async insert is only supported for the materialized view with the inner query which produces no more rows than in the input. "
            " You can either use sync inserts with deduplicaton or disable deduplication in dependent materialised views with async insert (deduplicate_blocks_in_dependent_materialized_views=0). "
            " Inner query of the materialized view '{}' produced more that one block. "
            " debug: {}",
            visited_views.back(),
            debug());
}


DeduplicationInfo::FilterResult DeduplicationInfo::deduplicateBlock(
    const std::vector<std::string> & existing_block_ids, const std::string & partition_id, ContextPtr context) const
{
    chassert(!existing_block_ids.empty());
    chassert(!disabled);
    return recalculateBlock(filterImpl(filterOriginal(existing_block_ids, partition_id)), partition_id, context);
}


const std::vector<StorageIDMaybeEmpty> & DeduplicationInfo::getVisitedViews() const
{
    return visited_views;
}


void DeduplicationInfo::setSourceBlockNumber(size_t block_number)
{
    chassert(level == Level::SOURCE);
    addExtraPart(TokenDefinition::Extra::asSourceNumber(block_number));
}


size_t DeduplicationInfo::getTokenBegin(size_t pos) const
{
    chassert(pos < offsets.size());
    if (pos == 0)
        return 0;
    return offsets[pos - 1];
}


size_t DeduplicationInfo::getTokenEnd(size_t pos) const
{
    chassert(pos < offsets.size());
    return offsets[pos];
}


size_t DeduplicationInfo::getTokenRows(size_t pos) const
{
    return getTokenEnd(pos) - getTokenBegin(pos);
}


void DeduplicationInfo::addExtraPart(const TokenDefinition::Extra & extra)
{
    for (auto & token : tokens)
        token.extra_tokens.push_back(extra);
}


bool DeduplicationInfo::TokenDefinition::empty() const
{
    return by_user.empty() && !by_part_writer.has_value();
}


bool DeduplicationInfo::TokenDefinition::operator==(const TokenDefinition & other) const
{
    return by_user == other.by_user && by_part_writer == other.by_part_writer && data_hash == other.data_hash && extra_tokens == other.extra_tokens;
}


ChunkInfo::Ptr DeduplicationInfo::merge(const ChunkInfo::Ptr & right) const
{
    return std::static_pointer_cast<ChunkInfo>(mergeSelf(std::static_pointer_cast<DeduplicationInfo>(right)));
}


DeduplicationInfo::Ptr DeduplicationInfo::mergeSelf(const Ptr & right) const
{
    chassert(right);

    LOG_DEBUG(
        logger,
        "Merging:\n left: {}\n right: {}\n"
        , debug()
        , right->debug());

    chassert(disabled == right->disabled);
    chassert(is_async_insert == right->is_async_insert);
    chassert(this->visited_views == right->visited_views);

    if (!disabled && is_async_insert && visited_views.size() > 1)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Merging deduplication infos with more than one visited view is not supported with deduplication on async insert, left: {}, right: {}",
            debug(),
            right->debug());

    if (!disabled && !is_async_insert && visited_views.size() > 2)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Merging deduplication infos with more than two visited view is not supported with deduplication on sync insert, left: {}, right: {}",
            debug(),
            right->debug());

    auto new_instance = DeduplicationInfo::cloneMergeImpl();

    auto do_extend = [&] ()
    {
        chassert(this->getCount() == 1);
        chassert(right->getCount() == 1);
        new_instance->tokens.push_back(this->tokens[0]);
        new_instance->tokens.back().doExtend(right->tokens[0]);
        new_instance->offsets.push_back(this->getRows() + right->getRows());
        if (new_instance->level == Level::SOURCE)
            new_instance->tokens.back().data_hash.reset(); // reset data hash because the last block has changed and data hash should be recalculated later if it is needed
    };

    auto do_concat = [&] ()
    {
        // concat tokens
        new_instance->tokens.reserve(this->tokens.size() + right->tokens.size());
        new_instance->tokens.insert(new_instance->tokens.end(), this->tokens.begin(), this->tokens.end());
        new_instance->tokens.insert(new_instance->tokens.end(), right->tokens.begin(), right->tokens.end());

        // concat offsets
        new_instance->offsets.reserve(this->offsets.size() + right->offsets.size());
        new_instance->offsets.insert(new_instance->offsets.end(), this->offsets.begin(), this->offsets.end());
        size_t rows = new_instance->getRows(); // correct offset for right part
        for (const auto & offset : right->offsets)
            new_instance->offsets.push_back(rows + offset);

        chassert(new_instance->tokens.size() == new_instance->offsets.size());
    };

    if (this->getCount() == 1 && right->getCount() == 1 && this->tokens[0].canBeExtended(right->tokens[0]))
    {
        // this is a sqush when both sides have the same token for all rows
        do_extend();
    }
    else
    {
        do_concat();
    }

    LOG_DEBUG(
        logger,
        "Merged: {}",
        new_instance->debug());

    return new_instance;
}


std::string DeduplicationInfo::TokenDefinition::Extra::toStringImpl(bool debug) const
{
    auto to_str = [] (const StorageIDMaybeEmpty & id) -> std::string
    {
       if (id.empty())
           return "empty";
       return id.hasUUID() ? DB::toString(id.uuid) : id.getFullNameNotQuoted();
    };

    switch (type)
    {
        case SOURCE_ID:
            return "source-id-" + to_str(std::get<StorageIDMaybeEmpty>(value_variant));
        case SOURCE_NUMBER:
            {
                const auto & range = std::get<Range>(value_variant);
                if (range.first + 1 == range.second)
                    return "source-number-" + DB::toString(range.first);
                else if (debug)
                    return "source-number-" + DB::toString(range.first) + "-" + DB::toString(range.second);
                else
                    return "source-number-" + DB::toString(range.second - 1);
            }
        case VIEW_ID:
            return "view-id-" + to_str(std::get<StorageIDMaybeEmpty>(value_variant));
        case VIEW_NUMBER:
            {
                const auto & range = std::get<Range>(value_variant);
                if (range.first + 1 == range.second)
                    return "view-block-" + DB::toString(range.first);
                else if (debug)
                    return "view-block-" + DB::toString(range.first) + "-" + DB::toString(range.second);
                else
                    return "view-block-" + DB::toString(range.second - 1);
            }
    }
    UNREACHABLE();
}


std::string DeduplicationInfo::TokenDefinition::Extra::debug() const
{
    return toStringImpl(true);
}


std::string DeduplicationInfo::TokenDefinition::Extra::toString() const
{
    return toStringImpl(false);
}


std::string DeduplicationInfo::TokenDefinition::debug() const
{
    std::string str;

    if (!by_user.empty())
        str = fmt::format("user<{}>", by_user);
    else if (by_part_writer.has_value())
        str = fmt::format("data-hash<{}_{}>", by_part_writer->items[0], by_part_writer->items[1]);
    else
        str = "-";

    for (const auto & extra : extra_tokens)
    {
        str += "/";
        str += extra.debug();
    }

    return str;
}


bool DeduplicationInfo::TokenDefinition::Extra::operator==(const Extra & other) const
{
    return type == other.type && value_variant == other.value_variant;
}


DeduplicationInfo::TokenDefinition::Extra DeduplicationInfo::TokenDefinition::Extra::asSourceID(const StorageIDMaybeEmpty & id)
{
    return Extra{SOURCE_ID, id};
}


DeduplicationInfo::TokenDefinition::Extra DeduplicationInfo::TokenDefinition::Extra::asSourceNumber(uint64_t number)
{
    return Extra{SOURCE_NUMBER, Range{number, number + 1}};
}


DeduplicationInfo::TokenDefinition::Extra DeduplicationInfo::TokenDefinition::Extra::asViewID(const StorageIDMaybeEmpty & id)
{
    return Extra{VIEW_ID, id};
}


DeduplicationInfo::TokenDefinition::Extra DeduplicationInfo::TokenDefinition::Extra::asViewNumber(uint64_t number)
{
    return Extra{VIEW_NUMBER, Range{number, number + 1}};
}


bool DeduplicationInfo::TokenDefinition::canBeExtended(const TokenDefinition & right) const
{
    LOG_TEST(getLogger("canBeExtended"), "{} vs {}", this->debug(), right.debug());

    if (by_user != right.by_user || by_part_writer != right.by_part_writer)
        return false;

    if (extra_tokens.size() != right.extra_tokens.size())
        return false;

    if (extra_tokens.empty())
        return true;

    const auto & left_last_extra = extra_tokens.back();
    const auto & right_last_extra = right.extra_tokens.back();

    if (left_last_extra.type != right_last_extra.type)
        return false;

    if (left_last_extra != right_last_extra)
    {
        // type is equal but values are different

        switch (left_last_extra.type)
        {
            case Extra::Type::SOURCE_ID:
            case Extra::Type::VIEW_ID:
                return false;
            case Extra::Type::SOURCE_NUMBER:
            case Extra::Type::VIEW_NUMBER: {
                const auto & left_range = std::get<Extra::Range>(left_last_extra.value_variant);
                const auto & right_range = std::get<Extra::Range>(right_last_extra.value_variant);
                // check if ranges are continuous
                if (left_range.second == right_range.first)
                    break; // continue and check other extras
                return false;
            }
        }
    }

    for (size_t i = 0; i < extra_tokens.size() - 1; ++i)
    {
        if (extra_tokens[i] != right.extra_tokens[i])
            return false;
    }

    return true;
}


void DeduplicationInfo::TokenDefinition::doExtend(const TokenDefinition & right)
{
    chassert(canBeExtended(right));

    if (extra_tokens.empty())
        return;

    auto & left_last_extra = extra_tokens.back();
    const auto & right_last_extra = right.extra_tokens.back();

    if (left_last_extra == right_last_extra)
        return;

    data_hash.reset(); // invalidate data hash as token is changed

    // type is equal but values are different
    switch (left_last_extra.type)
    {
        case Extra::Type::SOURCE_ID:
        case Extra::Type::VIEW_ID:
            chassert(false);
            break;
        case Extra::Type::SOURCE_NUMBER:
        case Extra::Type::VIEW_NUMBER: {
            auto & left_range = std::get<Extra::Range>(left_last_extra.value_variant);
            const auto & right_range = std::get<Extra::Range>(right_last_extra.value_variant);
            // extend range
            chassert(left_range.second == right_range.first);
            left_range.second = right_range.second;
            break;
        }
    }
}

}
