
#include <Storages/StorageProxy.h>
#include <Storages/StorageMergeTreeTextIndex.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Core/Range.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ITokenizer.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/TextIndexBlockedPositionsCodec.h>
#include <Storages/VirtualColumnUtils.h>
#include <Access/Common/AccessFlags.h>
#include <Storages/getEffectiveRowPolicyFilter.h>

#include <numeric>

namespace DB
{

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int BAD_ARGUMENTS;
    extern const int CORRUPTED_DATA;
    extern const int FILE_DOESNT_EXIST;
}

/// Reads the posting lists of the tokens of a part a dictionary block at a time, segment by segment of each posting list,
/// with the positions of the token in the rows if they are requested.
struct SimplePostingsCursor
{
    /// Reads the posting lists of a part from its files. `positions_buf_` is null if the positions are not read.
    SimplePostingsCursor(
        PostingsSerialization postings_serialization_,
        std::unique_ptr<ReadBufferFromFileBase> postings_buf_,
        std::unique_ptr<ReadBufferFromFileBase> positions_buf_,
        size_t positions_file_size_)
        : postings_serialization(std::move(postings_serialization_))
        , postings_buf(std::move(postings_buf_))
        , positions_buf(std::move(positions_buf_))
        , positions_file_size(positions_file_size_)
    {
    }

    /// Starts reading the posting lists of the tokens of `block_`, whose columns of the tokens are `block_columns_`.
    void setBlock(DictionaryBlock block_, MutableColumns block_columns_)
    {
        block = std::move(block_);
        block_columns = std::move(block_columns_);
        token_index = 0;
        segment_index = 0;
        segment_rank = 0;
    }

    /// Reads the next segment of a posting list of the block into `row_ids`, and into `positions` if they are read.
    /// Returns false if all posting lists of the block are read.
    bool readNextSegment()
    {
        if (!block)
            return false;

        while (token_index < block->size() && segment_index == block->token_infos[token_index].offsets.size())
        {
            ++token_index;
            segment_index = 0;
            segment_rank = 0;
        }

        if (token_index == block->size())
            return false;

        readSegment(block->token_infos[token_index]);
        return true;
    }

    /// The dictionary block whose posting lists are read, with its columns of the tokens (a row per token).
    std::optional<DictionaryBlock> block;
    MutableColumns block_columns;
    /// The token of the last read segment.
    size_t token_index = 0;

    /// Data of the last read segment of a posting list.
    PaddedPODArray<UInt32> row_ids;
    PaddedPODArray<UInt32> position_offsets;
    PaddedPODArray<UInt32> positions;

private:
    /// Reads the segment `segment_index` of the posting list of the current token, then moves to the next segment.
    void readSegment(const TokenPostingsInfo & token_info)
    {
        row_ids.clear();
        position_offsets.clear();
        positions.clear();

        if (token_info.header & PostingsSerialization::Flags::EmbeddedPostings)
        {
            row_ids.insert(token_info.embedded_postings.begin(), token_info.embedded_postings.end());
        }
        else
        {
            postings_buf->seek(static_cast<off_t>(token_info.offsets[segment_index]), SEEK_SET);
            postings_serialization.deserializeToArray(*postings_buf, token_info, segment_index, row_ids);
        }

        if (positions_buf)
            readPositions(token_info, segment_rank, row_ids.size());

        segment_rank += row_ids.size();
        ++segment_index;
    }

    /// Reads the positions of the postings [first_rank, first_rank + num_ranks) of the token into `positions`,
    /// delimited by the end offsets in `position_offsets`.
    void readPositions(const TokenPostingsInfo & token_info, size_t first_rank, size_t num_ranks)
    {
        static constexpr size_t block_docs = TextIndexBlockedPositionsCodec::BLOCK_DOCS;

        if (first_rank == 0)
        {
            validateTokenPositions(token_info, positions_file_size);
            positions_buf->seek(static_cast<off_t>(token_info.position_offset), SEEK_SET);
            positions_directory = TextIndexBlockedPositionsCodec::readDirectory(*positions_buf, token_info.position_offset, token_info.cardinality, token_info.position_bytes);
        }

        const size_t end_rank = first_rank + num_ranks;
        if (end_rank > positions_directory.num_docs)
        {
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "Corrupt text index positions: more posting documents than position lists ({})", positions_directory.num_docs);
        }

        for (size_t rank = first_rank; rank < end_rank;)
        {
            const size_t block_idx = rank / block_docs;
            const size_t block_end = std::min(end_rank, (block_idx + 1) * block_docs);

            block_local_ranks.resize(block_end - rank);
            std::iota(block_local_ranks.begin(), block_local_ranks.end(), static_cast<UInt32>(rank % block_docs));

            positions_buf->seek(static_cast<off_t>(positions_directory.block_offsets[block_idx]), SEEK_SET);
            TextIndexBlockedPositionsCodec::decodeBlock(*positions_buf, positions_directory, block_idx, block_local_ranks, position_offsets, positions, positions_scratch);
            rank = block_end;
        }
    }

    /// Files of the posting lists and positions of the current part.
    PostingsSerialization postings_serialization;
    std::unique_ptr<ReadBufferFromFileBase> postings_buf;
    std::unique_ptr<ReadBufferFromFileBase> positions_buf;
    size_t positions_file_size = 0;

    /// The next segment of the posting list of the current token, and the rank of its first posting in the list.
    size_t segment_index = 0;
    size_t segment_rank = 0;

    TextIndexBlockedPositionsCodec::Directory positions_directory;
    PaddedPODArray<UInt32> block_local_ranks;
    TextIndexBlockedPositionsCodec::DecodeScratch positions_scratch;
};

class MergeTreeTextIndexSource final : public ISource
{
public:
    MergeTreeTextIndexSource(
        SharedHeader header_,
        std::shared_ptr<MergeTreeData::DataPartsVector> data_parts_,
        std::shared_ptr<std::atomic<size_t>> part_index_,
        MergeTreeIndexPtr index_ptr,
        ReadSettings read_settings_,
        size_t max_block_size_,
        std::shared_ptr<const KeyCondition> token_key_condition_)
        : ISource(header_)
        , header(std::move(header_))
        , data_parts(std::move(data_parts_))
        , part_index(std::move(part_index_))
        , read_settings(std::move(read_settings_))
        , max_block_size(max_block_size_)
        , token_key_condition(std::move(token_key_condition_))
        , index_name(index_ptr->index.name)
    {
        const auto & text_index = typeid_cast<const MergeTreeIndexText &>(*index_ptr);

        for (const auto & substream : text_index.getSubstreams())
            index_streams[substream.type] = {text_index.getFileName() + substream.suffix, substream.extension};

        if (header->has("token_key"))
            token_key_pos = header->getPositionByName("token_key");

        if (header->has("token_value"))
            token_value_pos = header->getPositionByName("token_value");

        if (header->has("row_id"))
            row_id_pos = header->getPositionByName("row_id");

        if (header->has("positions"))
            positions_pos = header->getPositionByName("positions");

        read_postings = row_id_pos || positions_pos;
    }

    String getName() const override { return "MergeTreeTextIndex"; }

protected:
    Chunk generate() override
    {
        auto component_guard = Coordination::setCurrentComponent("MergeTreeTextIndexSource::generate");

        if (read_postings)
            return generateWithPostings();

        size_t total_rows = 0;
        MutableColumns result_columns = header->cloneEmptyColumns();

        for (auto & column : result_columns)
            column->reserve(max_block_size);

        /// Total rows may overflow the max_block_size.
        /// It is considered ok because dictionary block size
        /// is usually much smaller than the max_block_size.
        while (total_rows < max_block_size)
        {
            auto dict_block = readNextDictionaryBlock();
            if (!dict_block)
                break;

            fillTokenColumns(*dict_block, result_columns);
            total_rows += dict_block->size();
        }

        if (total_rows == 0)
            return {};

        return Chunk(std::move(result_columns), total_rows);
    }

private:
    /// Returns a row per posting of each token, like `ARRAY JOIN` of the posting list: the columns of the token are
    /// replicated, and the columns of the posting list are filled from it. Reads a posting list segment by segment,
    /// accumulating the segments of several tokens up to `max_block_size` rows, so a chunk may exceed it by one segment.
    Chunk generateWithPostings()
    {
        size_t total_rows = 0;
        size_t num_columns = header->columns();
        MutableColumns result_columns = header->cloneEmptyColumns();

        while (total_rows < max_block_size)
        {
            if (!postings_cursor || !postings_cursor->readNextSegment())
            {
                auto dict_block = readNextDictionaryBlock();
                if (!dict_block)
                    break;

                MutableColumns block_columns = header->cloneEmptyColumns();
                fillTokenColumns(*dict_block, block_columns);
                postings_cursor->setBlock(std::move(*dict_block), std::move(block_columns));
                continue;
            }

            const auto & row_ids = postings_cursor->row_ids;
            size_t num_rows = row_ids.size();

            for (size_t pos = 0; pos < num_columns; ++pos)
            {
                if (pos == row_id_pos)
                {
                    auto & row_id_data = assert_cast<ColumnUInt32 &>(*result_columns[pos]).getData();
                    row_id_data.insert(row_ids.begin(), row_ids.end());
                }
                else if (pos == positions_pos)
                {
                    auto & array_column = assert_cast<ColumnArray &>(*result_columns[pos]);
                    auto & data = assert_cast<ColumnUInt32 &>(array_column.getData()).getData();
                    auto & offsets = array_column.getOffsets();
                    const size_t base = data.size();

                    data.insert(postings_cursor->positions.begin(), postings_cursor->positions.end());
                    for (UInt32 offset : postings_cursor->position_offsets)
                        offsets.push_back(base + offset);
                }
                else
                {
                    result_columns[pos]->insertManyFrom(*postings_cursor->block_columns[pos], postings_cursor->token_index, num_rows);
                }
            }

            total_rows += num_rows;
        }

        if (total_rows == 0)
            return {};

        return Chunk(std::move(result_columns), total_rows);
    }

    /// Fills the columns of the tokens with a row per token of the dictionary block.
    void fillTokenColumns(const DictionaryBlock & dict_block, MutableColumns & columns) const
    {
        using enum PostingsSerialization::Flags;

        size_t block_size = dict_block.size();
        size_t num_columns = header->columns();

        /// Dispatch each output column and fill it from the entire dictionary block.
        for (size_t pos = 0; pos < num_columns; ++pos)
        {
            const auto & col_with_type = header->getByPosition(pos);
            const auto & column_name = col_with_type.name;

            if (pos == row_id_pos || pos == positions_pos || pos == token_key_pos || pos == token_value_pos)
            {
                /// These columns are filled separately.
                continue;
            }
            else if (column_name == "token")
            {
                columns[pos]->insertRangeFrom(*dict_block.tokens, 0, block_size);
            }
            else if (column_name == "cardinality")
            {
                auto & data = assert_cast<ColumnUInt64 &>(*columns[pos]).getData();
                for (size_t i = 0; i < block_size; ++i)
                    data.push_back(static_cast<UInt64>(dict_block.token_infos[i].cardinality));
            }
            else if (column_name == "part_name")
            {
                auto column = col_with_type.type->createColumnConst(block_size, current_part_name);
                columns[pos]->insertManyFrom(column->getDataColumn(), 0, block_size);
            }
            else if (column_name == "dictionary_compression")
            {
                auto column = col_with_type.type->createColumnConst(block_size, static_cast<Int8>(dict_block.tokens_format));
                columns[pos]->insertManyFrom(column->getDataColumn(), 0, block_size);
            }
            else if (column_name == "num_posting_blocks")
            {
                auto & data = assert_cast<ColumnUInt64 &>(*columns[pos]).getData();
                for (size_t i = 0; i < block_size; ++i)
                {
                    if (dict_block.token_infos[i].header & EmbeddedPostings)
                        data.push_back(static_cast<UInt64>(1));
                    else
                        data.push_back(static_cast<UInt64>(dict_block.token_infos[i].offsets.size()));
                }
            }
            else if (column_name == "has_embedded_postings")
            {
                auto & data = assert_cast<ColumnUInt8 &>(*columns[pos]).getData();
                for (size_t i = 0; i < block_size; ++i)
                    data.push_back(static_cast<UInt8>((dict_block.token_infos[i].header & EmbeddedPostings) != 0));
            }
            else if (column_name == "has_raw_postings")
            {
                auto & data = assert_cast<ColumnUInt8 &>(*columns[pos]).getData();
                for (size_t i = 0; i < block_size; ++i)
                    data.push_back(static_cast<UInt8>((dict_block.token_infos[i].header & RawPostings) != 0));
            }
            else if (column_name == "has_compressed_postings")
            {
                auto & data = assert_cast<ColumnUInt8 &>(*columns[pos]).getData();
                for (size_t i = 0; i < block_size; ++i)
                    data.push_back(static_cast<UInt8>((dict_block.token_infos[i].header & IsCompressed) != 0));
            }
        }

        fillDecodedTokenColumns(dict_block, columns);
    }

    /// Fill `token_key` and `token_value` (whichever are requested) from the block's tokens.
    /// Present only for the `keyValuePairs` tokenizer, whose every token decodes.
    void fillDecodedTokenColumns(const DictionaryBlock & dict_block, MutableColumns & result_columns) const
    {
        if (!token_key_pos && !token_value_pos)
            return;

        ColumnString * key_column = token_key_pos ? &assert_cast<ColumnString &>(*result_columns[*token_key_pos]) : nullptr;
        ColumnString * value_column = token_value_pos ? &assert_cast<ColumnString &>(*result_columns[*token_value_pos]) : nullptr;
        const size_t block_size = dict_block.size();

        for (size_t i = 0; i < block_size; ++i)
        {
            const std::string_view token = dict_block.tokens->getDataAt(i);
            const auto decoded = KeyValuePairsTokenizer::decodeToken(token);

            if (key_column)
                key_column->insertData(decoded.key.data(), decoded.key.size());

            if (value_column)
                value_column->insertData(decoded.value.data(), decoded.value.size());
        }
    }

    /// Read the next dictionary block.
    /// Returns std::nullopt when all parts are exhausted.
    std::optional<DictionaryBlock> readNextDictionaryBlock()
    {
        while (true)
        {
            if (!dictionary_buf)
            {
                if (!advanceToNextPart())
                    return std::nullopt;
            }

            /// Use sparse index to seek directly to matching blocks.
            if (token_key_condition)
            {
                if (next_matching_block >= matching_blocks.size())
                {
                    dictionary_buf.reset();
                    continue;
                }

                size_t block_idx = matching_blocks[next_matching_block++];
                dictionary_buf->seek(index_header.sparse_index.getOffsetInFile(block_idx), 0);
                return TextIndexSerialization::deserializeDictionaryBlock(*dictionary_buf, /*with_postings=*/ read_postings);
            }
            else /// Sequential reading without filtering.
            {
                if (dictionary_buf->eof())
                {
                    dictionary_buf.reset();
                    continue;
                }

                return TextIndexSerialization::deserializeDictionaryBlock(*dictionary_buf, /*with_postings=*/ read_postings);
            }
        }
    }

    /// Opens the file of the index substream of `type` in `part` and returns it with its size.
    /// Returns a null buffer if the index is not materialized in the part.
    std::pair<std::unique_ptr<ReadBufferFromFileBase>, size_t> openIndexFile(const IMergeTreeDataPart & part, MergeTreeIndexSubstream::Type type) const
    {
        const auto & [stream_name, extension] = index_streams.at(type);

        /// Check for both original and hashed filenames (hashed if the index name is too long).
        auto actual_stream_name = IMergeTreeDataPart::getStreamNameOrHash(stream_name, extension, part.checksums);
        if (!actual_stream_name)
            return {};

        auto file_name = *actual_stream_name + extension;
        size_t file_size = part.checksums.files.at(file_name).file_size;
        return {part.getDataPartStorage().readFile(file_name, read_settings, file_size), file_size};
    }

    /// Creates the cursor over the posting lists of the part from the files of the posting lists and positions,
    /// whose index must store the data of the requested columns.
    void initPostingsCursor(const IMergeTreeDataPart & part)
    {
        if (positions_pos && !index_header.has_positions)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cannot read the positions: the text index '{}' in part '{}' was written without positions. "
                "Run `ALTER TABLE ... MATERIALIZE INDEX {}` to rebuild it",
                index_name, part.name, index_name);
        }

        auto postings_buf = openIndexFile(part, MergeTreeIndexSubstream::Type::TextIndexPostings).first;
        if (!postings_buf)
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Text index '{}' in part '{}' has no file with posting lists", index_name, part.name);

        std::unique_ptr<ReadBufferFromFileBase> positions_buf;
        size_t positions_file_size = 0;

        if (positions_pos)
        {
            std::tie(positions_buf, positions_file_size) = openIndexFile(part, MergeTreeIndexSubstream::Type::TextIndexPositions);
            if (!positions_buf)
                throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Text index '{}' in part '{}' has no file with positions", index_name, part.name);
        }

        PostingsSerialization postings_serialization(PostingListCodecFactory::createPostingListCodec(index_header.codec_type), index_header.version);
        postings_cursor.emplace(std::move(postings_serialization), std::move(postings_buf), std::move(positions_buf), positions_file_size);
    }

    /// Loads the header of the index in `part` if it is needed, and finds the dictionary blocks matching the token condition.
    /// Returns false if the part must be skipped: the index is not materialized in the part or no block matches.
    bool loadHeader(const IMergeTreeDataPart & part)
    {
        if (!token_key_condition && !read_postings)
            return true;

        auto idx_file = openIndexFile(part, MergeTreeIndexSubstream::Type::Regular).first;

        /// Skip part if index is not materialized in the part.
        if (!idx_file)
            return false;

        CompressedReadBufferFromFile idx_buf(std::move(idx_file));

        /// Without a token condition only the version and codecs are needed to read the posting lists.
        if (token_key_condition)
        {
            index_header = TextIndexSerialization::deserializeHeader(idx_buf);

            computeMatchingBlocks();
            return !matching_blocks.empty();
        }

        index_header = TextIndexSerialization::deserializeHeaderPrefix(idx_buf);
        return true;
    }

    /// Claim and open index files for the next part that has this text index.
    /// Uses atomic counter for lock-free work distribution across sources.
    /// Returns true if index files were opened, false if no more parts.
    bool advanceToNextPart()
    {
        dictionary_buf.reset();
        index_header = {};
        matching_blocks.clear();
        next_matching_block = 0;
        postings_cursor.reset();

        while (true)
        {
            size_t idx = part_index->fetch_add(1, std::memory_order_relaxed);
            if (idx >= data_parts->size())
                return false;

            const auto & part = (*data_parts)[idx];
            if (!loadHeader(*part))
                continue;

            auto dict_file = openIndexFile(*part, MergeTreeIndexSubstream::Type::TextIndexDictionary).first;
            /// Skip part if index is not materialized in the part.
            if (!dict_file)
                continue;

            if (read_postings)
                initPostingsCursor(*part);

            dictionary_buf = std::make_unique<CompressedReadBufferFromFile>(std::move(dict_file));
            current_part_name = part->name;
            return true;
        }
    }

    /// Use the KeyCondition against the sparse index to find which dictionary blocks match.
    void computeMatchingBlocks()
    {
        size_t num_blocks = index_header.sparse_index.size();
        auto string_type = std::make_shared<DataTypeString>();
        DataTypes key_types = {string_type};

        /// FieldRef can reference a column cell by pointer, avoiding string copies.
        /// The sparse index is loaded without a cache here, so tokens are stored as a raw column.
        ColumnsWithTypeAndName ref_columns = {{index_header.sparse_index.getTokensColumn(), string_type, ""}};

        for (size_t i = 0; i < num_blocks; ++i)
        {
            FieldRef left_ref(&ref_columns, i, 0);

            Range token_range = (i + 1 < num_blocks)
                ? Range(left_ref, true, FieldRef(&ref_columns, i + 1, 0), false)
                : Range::createLeftBounded(left_ref, true);

            Hyperrectangle hyperrectangle = {token_range};
            if (token_key_condition->checkInHyperrectangle(hyperrectangle, key_types).can_be_true)
                matching_blocks.push_back(i);
        }
    }

    SharedHeader header;
    std::shared_ptr<MergeTreeData::DataPartsVector> data_parts;
    std::shared_ptr<std::atomic<size_t>> part_index;
    ReadSettings read_settings;
    size_t max_block_size;
    std::shared_ptr<const KeyCondition> token_key_condition;
    String index_name;
    std::map<MergeTreeIndexSubstream::Type, std::pair<String, String>> index_streams;

    /// Positions of `token_key` / `token_value` in the header, if requested.
    std::optional<size_t> token_key_pos;
    std::optional<size_t> token_value_pos;

    /// Positions of the columns of the posting lists in the header, if requested.
    std::optional<size_t> row_id_pos;
    std::optional<size_t> positions_pos;
    bool read_postings = false;

    /// State for current part
    String current_part_name;
    std::unique_ptr<CompressedReadBufferFromFile> dictionary_buf;

    /// State for KeyCondition-based block filtering
    TextIndexHeader index_header;
    std::vector<size_t> matching_blocks;
    size_t next_matching_block = 0;

    /// State for reading the posting lists.
    std::optional<SimplePostingsCursor> postings_cursor;
};

class ReadFromMergeTreeTextIndex : public SourceStepWithFilter
{
public:
    std::string getName() const override { return "ReadFromMergeTreeTextIndex"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    ReadFromMergeTreeTextIndex(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        SharedHeader sample_block,
        std::shared_ptr<StorageMergeTreeTextIndex> storage_,
        size_t max_block_size_,
        size_t num_streams_)
        : SourceStepWithFilter(
            std::move(sample_block),
            column_names_,
            query_info_,
            storage_snapshot_,
            context_)
        , storage(std::move(storage_))
        , max_block_size(max_block_size_)
        , num_streams(num_streams_)
    {
    }

    void applyFilters(ActionDAGNodes added_filter_nodes) override;

private:
    std::shared_ptr<StorageMergeTreeTextIndex> storage;
    size_t max_block_size;
    size_t num_streams;
    ExpressionActionsPtr virtual_columns_filter;
    std::shared_ptr<const KeyCondition> token_key_condition;
};

void ReadFromMergeTreeTextIndex::applyFilters(ActionDAGNodes added_filter_nodes)
{
    SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));

    if (filter_actions_dag)
    {
        Block block_to_filter
        {
            { {}, std::make_shared<DataTypeString>(), StorageMergeTreeTextIndex::part_name_column.name },
        };

        auto dag = VirtualColumnUtils::splitFilterDagForAllowedInputs(filter_actions_dag->getOutputs().at(0), &block_to_filter, context);
        if (dag)
            virtual_columns_filter = VirtualColumnUtils::buildFilterExpression(std::move(*dag), context);

        /// Build a KeyCondition for the `token` column to skip dictionary blocks
        /// whose token range does not match the filter.
        ActionsDAGWithInversionPushDown inverted_dag(filter_actions_dag->getOutputs().at(0), context, /* boolean_context */ true);

        auto token_column = ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "token");
        auto key_expr = std::make_shared<ExpressionActions>(ActionsDAG({token_column}));
        auto key_condition = std::make_shared<KeyCondition>(inverted_dag, context, Names{"token"}, key_expr);

        if (!key_condition->alwaysUnknownOrTrue())
            token_key_condition = std::move(key_condition);
    }
}

void ReadFromMergeTreeTextIndex::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    /// Taken at read time: the storage outlives the query in a table created from the function before that was forbidden.
    /// NOLINT(storage-cast): the table function resolves the source table before building this.
    auto data_parts = dynamic_cast<const MergeTreeData &>(*storage->source_table).getDataPartsVectorForInternalUsage();
    std::erase_if(data_parts, [](const MergeTreeData::DataPartPtr & part) { return part->isEmpty(); });
    auto filtered_parts = VirtualColumnUtils::filterDataPartsWithExpression(data_parts, virtual_columns_filter);

    if (filtered_parts.empty())
    {
        pipeline.init(Pipe(std::make_shared<NullSource>(getOutputHeader())));
        return;
    }

    auto shared_part_index = std::make_shared<std::atomic<size_t>>(0);
    auto filtered_parts_ptr = std::make_shared<MergeTreeData::DataPartsVector>(std::move(filtered_parts));

    Pipes pipes;
    size_t actual_streams = std::min(num_streams, filtered_parts_ptr->size());
    pipes.reserve(actual_streams);

    for (size_t i = 0; i < actual_streams; ++i)
    {
        pipes.emplace_back(std::make_shared<MergeTreeTextIndexSource>(
            getOutputHeader(),
            filtered_parts_ptr,
            shared_part_index,
            storage->text_index,
            context->getReadSettings(),
            max_block_size,
            token_key_condition));
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));

    for (const auto & processor : pipe.getProcessors())
        processor->setStorageLimits(query_info.storage_limits);

    pipeline.init(std::move(pipe));
}

const ColumnWithTypeAndName StorageMergeTreeTextIndex::part_name_column{std::make_shared<DataTypeString>(), "part_name"};

StorageMergeTreeTextIndex::StorageMergeTreeTextIndex(
    const StorageID & table_id_,
    const StoragePtr & source_table_,
    MergeTreeIndexPtr text_index_,
    const ColumnsDescription & columns)
    : StorageWithCommonVirtualColumns(table_id_)
    , source_table(source_table_)
    , text_index(std::move(text_index_))
{
    if (!castStorage<MergeTreeData>(source_table, DeferredTable::Load))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Storage MergeTreeTextIndex expected MergeTree table, got: {}", source_table->getName());

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns);
    storage_metadata.setVirtuals(createVirtuals(typeid_cast<const MergeTreeIndexText &>(*text_index)));
    setInMemoryMetadata(storage_metadata);
}

VirtualColumnsDescription StorageMergeTreeTextIndex::createVirtuals(const MergeTreeIndexText & text_index)
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);

    desc.addEphemeral("row_id",
        std::make_shared<DataTypeUInt32>(),
        "Number of the row in the data part from the posting list of the token", VirtualsMaterializationPlace::Reader);

    if (text_index.params.enable_positions)
    {
        desc.addEphemeral("positions",
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt32>()),
            "Positions of the token in the row", VirtualsMaterializationPlace::Reader);
    }

    return desc;
}

void StorageMergeTreeTextIndex::checkAccess(const ContextPtr & context, const IStorage & source_table, const IMergeTreeIndex & index)
{
    const auto source_storage_id = source_table.getStorageID();
    /// The checks below are for the user who runs the query, so a shard of a distributed query may run it only as the
    /// initiating user: authenticated by the interserver secret, or reached by `remote(...)` as the same user, which the
    /// initiator confirms by pushing its roles (it does not when it rewrote the initial user to the connection user).
    const auto & client_info = context->getClientInfo();
    const bool same_user = client_info.initial_user == client_info.current_user && client_info.current_roles.has_value();
    if (client_info.query_kind == ClientInfo::QueryKind::SECONDARY_QUERY
        && client_info.interface != ClientInfo::Interface::TCP_INTERSERVER && !same_user)
        throw Exception(ErrorCodes::ACCESS_DENIED,
            "Table function `mergeTreeTextIndex` checks the access of the user who runs the query, so a shard of a "
            "distributed query can execute it only as the initiating user: through a cluster with an interserver secret, "
            "or through `remote` with the credentials of that user");

    context->checkAccess(AccessType::SELECT, source_storage_id, index.getColumnsRequiredForIndexCalc());

    /// The index is built over all rows of a part, so it contains tokens of the rows a row policy hides,
    /// regardless of which columns the policy filters on. The policy cannot be applied to the dictionary.
    if (getEffectiveRowPolicyFilter(source_table, context))
        throw Exception(ErrorCodes::ACCESS_DENIED,
            "Cannot read from `mergeTreeTextIndex` because a row policy is applied on table {}. "
            "The text index covers all rows of the table, so reading its tokens would violate the row policy",
            source_storage_id.getNameForLogs());
}

void StorageMergeTreeTextIndex::readImpl(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum,
    size_t max_block_size,
    size_t num_streams)
{
    checkAccess(context, *source_table, *text_index);

    auto sample_block = std::make_shared<const Block>(storage_snapshot->getSampleBlockForColumns(column_names));
    auto this_ptr = std::static_pointer_cast<StorageMergeTreeTextIndex>(shared_from_this());

    auto reading = std::make_unique<ReadFromMergeTreeTextIndex>(
        column_names,
        query_info,
        storage_snapshot,
        std::move(context),
        std::move(sample_block),
        std::move(this_ptr),
        max_block_size,
        num_streams);

    query_plan.addStep(std::move(reading));
}

}

