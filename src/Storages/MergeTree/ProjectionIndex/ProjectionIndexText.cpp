#include <Storages/MergeTree/ProjectionIndex/ProjectionIndexText.h>

#include <Columns/ColumnAggregateFunction.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Interpreters/ITokenExtractor.h>
#include <Interpreters/sortBlock.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTProjectionDeclaration.h>
#include <Parsers/ASTProjectionSelectQuery.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeIndexTextPreprocessor.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/ProjectionIndex/MergeTreeIndexProjection.h>
#include <Storages/MergeTree/ProjectionIndex/PostingListState.h>
#include <Storages/ProjectionsDescription.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/quoteString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int SUPPORT_IS_DISABLED;
}

MergeTreeIndexPtr textIndexCreator(const IndexDescription & index);
void textIndexValidator(const IndexDescription & index, bool /*attach*/);

ProjectionIndexPtr ProjectionIndexText::create(const ASTProjectionDeclaration & proj)
{
    auto index_ast = std::make_shared<ASTIndexDeclaration>(proj.index->clone(), proj.type->clone(), proj.name);
    index_ast->granularity = 100000000; /// Magic number used by text index
    return std::make_shared<ProjectionIndexText>(std::move(index_ast));
}

void ProjectionIndexText::fillProjectionDescription(
    ProjectionDescription & result, const IAST * /* index_expr */, const ColumnsDescription & columns, ContextPtr query_context) const
{
    chassert(result.index.get() == this);
    chassert(!index);

    /// TODO(amos): This interface need some better way to do abstraction, it fills itself (result.index is "this"), too hacky.
    static_cast<ProjectionIndexText &>(*result.index).index_description
        = IndexDescription::getIndexFromAST(index_ast, columns, /* is_implicitly_created */ true, query_context);
    /// TODO(amos): this also should be moved out to check whether `attach` or not
    textIndexValidator(index_description, true /* attach */);
    static_cast<ProjectionIndexText &>(*result.index).index = std::make_shared<MergeTreeIndexProjection>(
        result, std::static_pointer_cast<const MergeTreeIndexText>(textIndexCreator(index_description)));

    result.required_columns = index_description.expression->getRequiredColumns();
    if (std::find(result.required_columns.begin(), result.required_columns.end(), "_part_offset") == result.required_columns.end())
        result.required_columns.push_back("_part_offset");

    result.with_parent_part_offset = true;
    StorageInMemoryMetadata metadata;
    metadata.partition_key = KeyDescription::buildEmptyKey();

    result.type = ProjectionDescription::Type::Aggregate;
    result.sample_block_for_keys.insert({ColumnString::create(), std::make_shared<DataTypeString>(), "term"});
    auto posting_list_type
        = createPostingListType(index_ast->as<ASTIndexDeclaration>()->getType()->arguments, POSTING_LIST_FORMAT_VERSION_INITIAL);
    result.sample_block
        = {result.sample_block_for_keys.getByPosition(0), {posting_list_type->createColumn(), posting_list_type, "posting"}};

    ColumnsDescription projection_columns(result.sample_block.getNamesAndTypesList());
    /// TODO(amos): Add some setting to customize codec
    // projection_columns.modify(
    //     "term", [&](ColumnDescription & column) { column.codec = makeASTFunction("CODEC", std::make_shared<ASTIdentifier>("ZSTD")); });

    // projection_columns.modify(
    //     "posting_list",
    //     [&](ColumnDescription & column) { column.codec = makeASTFunction("CODEC", std::make_shared<ASTIdentifier>("ZSTD")); });

    auto term_ident = std::make_shared<ASTIdentifier>("term");
    metadata.sorting_key = KeyDescription::getSortingKeyFromAST(term_ident, projection_columns, query_context, {});
    metadata.primary_key = KeyDescription::getKeyFromAST(term_ident, projection_columns, query_context);
    metadata.primary_key.definition_ast = nullptr;
    metadata.setColumns(std::move(projection_columns));

    result.metadata = std::make_shared<StorageInMemoryMetadata>(metadata);
}

std::shared_ptr<MergeTreeSettings> ProjectionIndexText::getDefaultSettings() const
{
    auto settings = std::make_shared<MergeTreeSettings>();

    /// Same default as text index v3
    settings->set("compact_parts_flush_per_column", false);
    settings->set("write_marks_for_substreams_in_compact_parts", false);
    settings->set("index_granularity", index->text_index->params.dictionary_block_size);
    /// TODO(amos): Maybe adaptive is better
    settings->set("index_granularity_bytes", std::numeric_limits<UInt64>::max());

    /// Always use compact part
    settings->set("min_bytes_for_wide_part", std::numeric_limits<UInt64>::max());
    settings->set("min_level_for_wide_part", 0);
    settings->set("min_rows_for_wide_part", 0);

    /// No sparse
    settings->set("ratio_of_defaults_for_sparse_serialization", 1.0);


    settings->set("serialization_info_version", "with_types");
    settings->set("string_serialization_version", "with_size_stream"); /// TODO, use fc here

    /// Text projections only store 'term' and 'posting' columns, keeping file names short and predictable. Disabling
    /// filename hashing avoids complexity that this projection type does not yet support and prevents potential logical
    /// issues during heavy stress or fuzzy testing.
    settings->set("replace_long_file_name_to_hash", 0);

    return settings;
}

const IndexDescription * ProjectionIndexText::getIndexDescription() const
{
    return &index_description;
}

MergeTreeIndexPtr ProjectionIndexText::getIndex() const
{
    return std::static_pointer_cast<const IMergeTreeIndex>(index);
}

namespace
{

template <typename Input>
Block tokenize(
    const ITokenExtractor & extractor,
    const Input & input_data_column,
    UInt64 starting_offset,
    Block sample,
    const IColumn::Offsets * array_offsets,
    const ColumnUInt8::Container * null_map,
    const IColumn * index_column)
{
    alignas(16) uint8_t packed_buffer[128 * 4];
    size_t rows = array_offsets ? array_offsets->size() : (index_column ? index_column->size() : input_data_column.size());
    chassert(rows <= std::numeric_limits<UInt32>::max());

    StringHashMap<AggregateDataPtr> terms_hashing;
    StringHashMap<AggregateDataPtr>::LookupResult it;
    size_t terms_bytes = 0;

    auto terms = ColumnString::create();
    auto postings = sample.getByPosition(1).type->createColumn();
    ColumnAggregateFunction & posting_states = assert_cast<ColumnAggregateFunction &>(*postings);

    /// --------------------------
    /// Fixed PostingListData constants
    /// --------------------------
    constexpr size_t state_size = sizeof(PostingListData);
    constexpr size_t state_align = alignof(PostingListData);
    static_assert(state_size % state_align == 0, "PostingListData size must be multiple of its alignment");
    chassert(posting_states.getAggregateFunction()->sizeOfData() == state_size);
    chassert(posting_states.getAggregateFunction()->alignOfData() == state_align);

    constexpr size_t rows_per_chunk = 8192;
    constexpr size_t fixed_arena_chunk_size = rows_per_chunk * state_size;

    /// Arena for posting lists
    Arena & arena = posting_states.createOrGetArena();
    ArenaPtr posting_arena = std::make_shared<Arena>(fixed_arena_chunk_size, 1, fixed_arena_chunk_size);
    posting_states.addArena(posting_arena);

    char * place = nullptr;
    size_t place_left = 0;

    /// Allocate exactly one PostingListData at a time
    auto allocate_place = [&]() -> char *
    {
        if (place_left == 0)
        {
            /// TODO(amos): alignedAlloc in arena can waste up to state_align memory.
            place = arena.alignedAlloc(fixed_arena_chunk_size, state_align);
            place_left = rows_per_chunk;
        }
        char * ret = place;
        place += state_size;
        place_left -= 1;
        return ret;
    };

    auto work = [&]<bool has_null, typename Index>(const Index * index)
    {
        std::string_view data;
        for (size_t row = 0; row < rows; ++row)
        {
            size_t begin_row = row;
            size_t end_row = row + 1;

            if (array_offsets)
            {
                begin_row = (*array_offsets)[row - 1];
                end_row = (*array_offsets)[row];
            }

            for (size_t i = begin_row; i < end_row; ++i)
            {
                if constexpr (has_null)
                {
                    if ((*null_map)[i])
                        continue;
                }

                if constexpr (std::is_same_v<Index, void>)
                    data = input_data_column.getDataAt(i);
                else
                    data = input_data_column.getDataAt(index->getUInt(i));

                if (data.empty())
                    continue;

                forEachTokenPadded(
                    extractor,
                    data.data(),
                    data.size(),
                    [&](const char * token_start, size_t token_length)
                    {
                        std::string_view term(token_start, token_length);
                        bool inserted;
                        terms_hashing.emplace(term, it, inserted);

                        if (inserted)
                        {
                            char * current_place = allocate_place();
                            new (current_place) PostingListData(PostingListKind::Writer);
                            it->getMapped() = current_place;
                            terms_bytes += token_length;
                        }

                        reinterpret_cast<PostingListData *>(it->getMapped())
                            ->writer.add(starting_offset + row, posting_arena.get(), packed_buffer);

                        return false;
                    });
            }
        }
    };

    if (null_map)
        work.template operator()<true, void>(nullptr);
    else if (!index_column)
        work.template operator()<false, void>(nullptr);
    else if (const auto * uint8 = checkAndGetColumn<ColumnUInt8>(index_column))
        work.template operator()<false>(uint8);
    else if (const auto * uint16 = checkAndGetColumn<ColumnUInt16>(index_column))
        work.template operator()<false>(uint16);
    else if (const auto * uint32 = checkAndGetColumn<ColumnUInt32>(index_column))
        work.template operator()<false>(uint32);
    else if (const auto * uint64 = checkAndGetColumn<ColumnUInt64>(index_column))
        work.template operator()<false>(uint64);
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected size of index type for low cardinality column");

    /// Construct empty projection part to filter empty column.
    /// TODO(amos): For non-string columns, we need non-default placeholder.
    if (terms_hashing.empty())
    {
        terms->insertDefault();
        postings->insertDefault();
    }
    else
    {
        std::vector<std::pair<std::string_view, AggregateDataPtr>> sorted_terms_hashing;
        sorted_terms_hashing.reserve(terms_hashing.size());

        terms_hashing.forEachValue([&](const auto & key, auto & mapped) { sorted_terms_hashing.emplace_back(key, mapped); });
        std::ranges::sort(sorted_terms_hashing, [](const auto & lhs, const auto & rhs) { return lhs.first < rhs.first; });

        auto & offsets = terms->getOffsets();
        auto & chars = terms->getChars();
        auto & vec = posting_states.getData();

        chars.reserve_exact(terms_bytes);
        offsets.reserve_exact(sorted_terms_hashing.size());
        vec.reserve_exact(sorted_terms_hashing.size());

        size_t last_offset = 0;
        for (const auto & [term, posting] : sorted_terms_hashing)
        {
            offsets.push_back(last_offset + term.size());
            last_offset = offsets.back();
            chars.insert_assume_reserved(term.data(), term.data() + term.size());
            vec.push_back(posting);
        }
    }

    sample.getByPosition(0).column = std::move(terms);
    sample.getByPosition(1).column = std::move(postings);

    return sample;
}

}

Block ProjectionIndexText::calculate(
    const ProjectionDescription & projection_desc,
    const Block & block,
    UInt64 starting_offset,
    ContextPtr /* context */,
    const IColumnPermutation * /* perm_ptr */) const
{
    OpenTelemetry::SpanHolder span("ProjectionIndexText::calculate");

    if (block.rows() > std::numeric_limits<UInt32>::max())
    {
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "Cannot build text index in part with {} rows. Materialization of text index is not supported for parts with more than {} rows",
            block.rows(),
            std::numeric_limits<UInt32>::max());
    }

    /// TODO: handle RowExistsColumn, this means mutations: materializing projection index
    /// Respect the _row_exists column.
    // if (block.has(RowExistsColumn::name))
    // {
    //     query_ast_copy = query_ast->clone();
    //     auto * select_row_exists = query_ast_copy->as<ASTSelectQuery>();
    //     if (!select_row_exists)
    //         throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot get ASTSelectQuery when adding _row_exists = 1. It's a bug");

    //     select_row_exists->setExpression(
    //         ASTSelectQuery::Expression::WHERE,
    //         makeASTOperator("equals", std::make_shared<ASTIdentifier>(RowExistsColumn::name), std::make_shared<ASTLiteral>(1)));
    // }

    const auto & index_column = block.getByName(index_description.column_names[0]);
    ColumnPtr doc_column = index_column.column;

    /// TODO(amos): We should also support preprocessor for array column
    if (!isArray(index_column.type))
    {
        auto agg = index->text_index->preprocessor;
        auto [processed_column, _] = index->text_index->preprocessor->processColumn(index_column, 0, index_column.column->size());
        doc_column = processed_column;
    }

    const IColumn::Offsets * array_offsets = nullptr;
    const ColumnUInt8::Container * null_map = nullptr;
    const IColumn * lowcard_index = nullptr;

    if (const auto * array = checkAndGetColumn<ColumnArray>(doc_column.get()))
    {
        array_offsets = &array->getOffsets();
        doc_column = array->getDataPtr();
    }

    if (const auto * nullable = checkAndGetColumn<ColumnNullable>(doc_column.get()))
    {
        null_map = &nullable->getNullMapData();
        doc_column = nullable->getNestedColumnPtr();
    }
    else if (const auto * low_card = checkAndGetColumn<ColumnLowCardinality>(doc_column.get()))
    {
        doc_column = low_card->getDictionary().getNestedNotNullableColumn();
        lowcard_index = &low_card->getIndexes();
    }

    const auto * column_string = checkAndGetColumn<ColumnString>(doc_column.get());
    const auto * column_fixed_string = checkAndGetColumn<ColumnFixedString>(doc_column.get());
    if (!column_string && !column_fixed_string)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Inverted index projection only accepts string columns for now");

    Block tokenized_block;
    if (column_string)
    {
        tokenized_block = tokenize(
            *index->text_index->token_extractor,
            *column_string,
            starting_offset,
            projection_desc.sample_block,
            array_offsets,
            null_map,
            lowcard_index);
    }
    else
    {
        tokenized_block = tokenize(
            *index->text_index->token_extractor,
            *column_fixed_string,
            starting_offset,
            projection_desc.sample_block,
            array_offsets,
            null_map,
            lowcard_index);
    }

    return tokenized_block;
}

}
