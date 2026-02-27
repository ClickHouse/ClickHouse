
#include <Storages/StorageMergeTreeTextIndex.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Core/Range.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/VirtualColumnUtils.h>
#include <Access/Common/AccessFlags.h>
#include <Access/EnabledRowPolicies.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int BAD_ARGUMENTS;
}

class MergeTreeTextIndexSource : public ISource
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
    {
        const auto & text_index = typeid_cast<const MergeTreeIndexText &>(*index_ptr);
        posting_list_codec = text_index.getPostingListCodec();

        for (const auto & substream : text_index.getSubstreams())
            index_streams[substream.type] = {text_index.getFileName() + substream.suffix, substream.extension};
    }

    String getName() const override { return "MergeTreeTextIndex"; }

protected:
    Chunk generate() override
    {
        using enum PostingsSerialization::Flags;

        size_t total_rows = 0;
        size_t num_columns = header->columns();
        MutableColumns result_columns = header->cloneEmptyColumns();

        /// Total rows may overflow the max_block_size.
        /// It is considered ok because dictionary block size
        /// is usually much smaller than the max_block_size.
        while (total_rows < max_block_size)
        {
            auto dict_block = readNextDictionaryBlock();
            if (!dict_block)
                break;

            size_t block_size = dict_block->size();

            /// Dispatch each output column and fill it from the entire dictionary block.
            for (size_t pos = 0; pos < num_columns; ++pos)
            {
                const auto & col_with_type = header->getByPosition(pos);
                const auto & column_name = col_with_type.name;

                if (column_name == "token")
                {
                    result_columns[pos]->insertRangeFrom(*dict_block->tokens, 0, block_size);
                }
                else if (column_name == "cardinality")
                {
                    auto & data = assert_cast<ColumnUInt64 &>(*result_columns[pos]).getData();
                    for (size_t i = 0; i < block_size; ++i)
                        data.push_back(static_cast<UInt64>(dict_block->token_infos[i].cardinality));
                }
                else if (column_name == "part_name")
                {
                    auto column = col_with_type.type->createColumnConst(block_size, current_part_name);
                    result_columns[pos]->insertManyFrom(assert_cast<const ColumnConst &>(*column).getDataColumn(), 0, block_size);
                }
                else if (column_name == "dictionary_compression")
                {
                    auto column = col_with_type.type->createColumnConst(block_size, static_cast<Int8>(dict_block->tokens_format));
                    result_columns[pos]->insertManyFrom(assert_cast<const ColumnConst &>(*column).getDataColumn(), 0, block_size);
                }
                else if (column_name == "num_posting_blocks")
                {
                    auto & data = assert_cast<ColumnUInt64 &>(*result_columns[pos]).getData();
                    for (size_t i = 0; i < block_size; ++i)
                        data.push_back(static_cast<UInt64>(dict_block->token_infos[i].offsets.size()));
                }
                else if (column_name == "has_embedded_postings")
                {
                    auto & data = assert_cast<ColumnUInt8 &>(*result_columns[pos]).getData();
                    for (size_t i = 0; i < block_size; ++i)
                        data.push_back(static_cast<UInt8>((dict_block->token_infos[i].header & EmbeddedPostings) != 0));
                }
                else if (column_name == "has_raw_postings")
                {
                    auto & data = assert_cast<ColumnUInt8 &>(*result_columns[pos]).getData();
                    for (size_t i = 0; i < block_size; ++i)
                        data.push_back(static_cast<UInt8>((dict_block->token_infos[i].header & RawPostings) != 0));
                }
                else if (column_name == "has_compressed_postings")
                {
                    auto & data = assert_cast<ColumnUInt8 &>(*result_columns[pos]).getData();
                    for (size_t i = 0; i < block_size; ++i)
                        data.push_back(static_cast<UInt8>((dict_block->token_infos[i].header & IsCompressed) != 0));
                }
            }

            total_rows += block_size;
        }

        if (total_rows == 0)
            return {};

        return Chunk(std::move(result_columns), total_rows);
    }

private:
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
                dictionary_buf->seek(sparse_index.getOffsetInFile(block_idx), 0);
                return TextIndexSerialization::deserializeDictionaryBlock(*dictionary_buf, posting_list_codec);
            }
            else /// Sequential reading without filtering.
            {
                if (dictionary_buf->eof())
                {
                    dictionary_buf.reset();
                    continue;
                }

                return TextIndexSerialization::deserializeDictionaryBlock(*dictionary_buf, posting_list_codec);
            }
        }
    }

    /// Claim and open index files for the next part that has this text index.
    /// Uses atomic counter for lock-free work distribution across sources.
    /// Returns true if index files were opened, false if no more parts.
    bool advanceToNextPart()
    {
        dictionary_buf.reset();
        sparse_index = {};
        matching_blocks.clear();
        next_matching_block = 0;

        while (true)
        {
            size_t idx = part_index->fetch_add(1, std::memory_order_relaxed);
            if (idx >= data_parts->size())
                return false;

            const auto & part = (*data_parts)[idx];
            const auto & storage = part->getDataPartStorage();

            if (token_key_condition)
            {
                auto [stream_name, extension] = index_streams.at(MergeTreeIndexSubstream::Type::Regular);

                /// Skip part if index is not materialized in the part.
                /// Check for both original and hashed filenames (hashed if the index name is too long).
                auto actual_sparse_index_name = IMergeTreeDataPart::getStreamNameOrHash(stream_name, extension, part->checksums);
                if (!actual_sparse_index_name)
                    continue;

                auto sparse_file_name = *actual_sparse_index_name + extension;
                auto idx_file = storage.readFile(sparse_file_name, read_settings, part->checksums.files.at(sparse_file_name).file_size);

                CompressedReadBufferFromFile idx_buf(std::move(idx_file));
                sparse_index = TextIndexSerialization::deserializeSparseIndex(idx_buf);

                if (sparse_index.empty())
                    continue;

                computeMatchingBlocks();
                if (matching_blocks.empty())
                    continue;
            }

            auto [stream_name, extension] = index_streams.at(MergeTreeIndexSubstream::Type::TextIndexDictionary);

            /// Skip part if index is not materialized in the part.
            /// Check for both original and hashed filenames (hashed if the index name is too long).
            auto actual_dict_name = IMergeTreeDataPart::getStreamNameOrHash(stream_name, extension, part->checksums);
            if (!actual_dict_name)
                continue;

            auto dict_file_name = *actual_dict_name + extension;
            auto dict_file = storage.readFile(dict_file_name, read_settings, part->checksums.files.at(dict_file_name).file_size);

            dictionary_buf = std::make_unique<CompressedReadBufferFromFile>(std::move(dict_file));
            current_part_name = part->name;
            return true;
        }
    }

    /// Use the KeyCondition against the sparse index to find which dictionary blocks match.
    void computeMatchingBlocks()
    {
        size_t num_blocks = sparse_index.size();
        auto string_type = std::make_shared<DataTypeString>();
        DataTypes key_types = {string_type};

        /// FieldRef can reference a column cell by pointer, avoiding string copies.
        ColumnsWithTypeAndName ref_columns = {{sparse_index.tokens, string_type, "token"}};

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
    PostingListCodecPtr posting_list_codec;
    ReadSettings read_settings;
    size_t max_block_size;
    std::shared_ptr<const KeyCondition> token_key_condition;
    std::map<MergeTreeIndexSubstream::Type, std::pair<String, String>> index_streams;

    /// State for current part
    String current_part_name;
    std::unique_ptr<CompressedReadBufferFromFile> dictionary_buf;

    /// State for KeyCondition-based block filtering
    DictionarySparseIndex sparse_index;
    std::vector<size_t> matching_blocks;
    size_t next_matching_block = 0;
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
        ActionsDAGWithInversionPushDown inverted_dag(filter_actions_dag->getOutputs().at(0), context);

        auto token_column = ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "token");
        auto key_expr = std::make_shared<ExpressionActions>(ActionsDAG({token_column}));
        auto key_condition = std::make_shared<KeyCondition>(inverted_dag, context, Names{"token"}, key_expr);

        if (!key_condition->alwaysUnknownOrTrue())
            token_key_condition = std::move(key_condition);
    }
}

void ReadFromMergeTreeTextIndex::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto filtered_parts = VirtualColumnUtils::filterDataPartsWithExpression(storage->data_parts, virtual_columns_filter);

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
    : IStorage(table_id_)
    , source_table(source_table_)
    , text_index(std::move(text_index_))
{
    const auto * merge_tree = dynamic_cast<const MergeTreeData *>(source_table.get());
    if (!merge_tree)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Storage MergeTreeTextIndex expected MergeTree table, got: {}", source_table->getName());

    data_parts = merge_tree->getDataPartsVectorForInternalUsage();
    std::erase_if(data_parts, [](const MergeTreeData::DataPartPtr & part) { return part->isEmpty(); });

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns);
    setInMemoryMetadata(storage_metadata);
}

void StorageMergeTreeTextIndex::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum,
    size_t max_block_size,
    size_t num_streams)
{
    auto source_storage_id = source_table->getStorageID();
    auto required_columns = text_index->getColumnsRequiredForIndexCalc();
    context->checkAccess(AccessType::SELECT, source_storage_id, required_columns);
    /// If the row policy filter references any column required for building the index,
    /// reading from the text index would expose tokens derived from those columnsand violate the row policy.
    auto row_policy_filter = context->getRowPolicyFilter(source_storage_id.getDatabaseName(), source_storage_id.getTableName(), RowPolicyFilterType::SELECT_FILTER);

    if (row_policy_filter && !row_policy_filter->isAlwaysTrue())
    {
        RequiredSourceColumnsVisitor::Data columns_context;
        RequiredSourceColumnsVisitor(columns_context).visit(row_policy_filter->expression);
        NameSet row_policy_columns = columns_context.requiredColumns();

        for (const auto & column_name : required_columns)
        {
            if (row_policy_columns.contains(column_name))
            {
                throw Exception(ErrorCodes::ACCESS_DENIED,
                    "Cannot read from `mergeTreeTextIndex` because a row policy on column `{}` "
                    "is applied on table {}. Reading text index tokens could violate the row policy",
                    column_name, source_storage_id.getNameForLogs());
            }
        }
    }

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

