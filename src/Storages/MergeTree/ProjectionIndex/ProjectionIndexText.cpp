#include <Storages/MergeTree/ProjectionIndex/ProjectionIndexText.h>

#include <Columns/ColumnAggregateFunction.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/ITokenizer.h>
#include <Interpreters/sortBlock.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTProjectionDeclaration.h>
#include <Parsers/ASTProjectionSelectQuery.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/IPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeIndexTextPreprocessor.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/ProjectionIndex/MergeTreeProjectionIndexText.h>
#include <Storages/MergeTree/ProjectionIndex/PostingListState.h>
#include <Storages/ProjectionsDescription.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/quoteString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int SUPPORT_IS_DISABLED;
    extern const int LIMIT_EXCEEDED;
}

MergeTreeIndexPtr textIndexCreator(const IndexDescription & index);
void textIndexValidator(const IndexDescription & index, bool /*attach*/);

ProjectionIndexPtr ProjectionIndexText::create(const ASTProjectionDeclaration & proj)
{
    auto index_expr = proj.index->clone();
    if (const auto * expr_list = index_expr->as<ASTExpressionList>(); expr_list && expr_list->children.size() == 1)
        index_expr = expr_list->children[0];
    else if (expr_list && expr_list->children.size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Projection text index must have exactly one indexed expression");

    auto index_ast = make_intrusive<ASTIndexDeclaration>(std::move(index_expr), proj.type->clone(), proj.name);
    /// Dummy value — projection text index does not use skip-index granularity,
    /// but ASTIndexDeclaration requires one. Only visible in EXPLAIN output.
    index_ast->granularity = 100'000'000;
    return std::make_shared<ProjectionIndexText>(std::move(index_ast));
}

void ProjectionIndexText::fillProjectionDescription(
    ProjectionDescription & result,
    const IAST * /* index_expr */,
    const ColumnsDescription & columns,
    const KeyDescription * /* partition_key */,
    const ContextPtr & query_context,
    const MergeTreeSettings & /* projection_settings */) const
{
    chassert(result.index.get() == this);
    chassert(!index);

    /// TODO(amos): This interface needs a better abstraction; it mutates itself (result.index is "this"), which is hacky.
    static_cast<ProjectionIndexText &>(*result.index).index_description = IndexDescription::getIndexFromAST(
        index_ast, columns, /* is_implicitly_created */ true, /* escape_filenames */ true, query_context);
    /// TODO(amos): this also should be moved out to check whether `attach` or not
    textIndexValidator(index_description, true /* attach */);

    /// Projection-specific shape validation: reject `Nullable(LowCardinality(...))` because the
    /// projection's tokenize fast path (see calculate()) requires either a per-row null map OR a
    /// LowCardinality dispatch, not both stacked. The canonical idiomatic form
    /// `LowCardinality(Nullable(...))` is supported (the dictionary's null map is projected onto
    /// a row-level null map). This keeps the shared skip-index validator unconstrained while
    /// preventing a column shape that would only fail later during INSERT.
    for (const auto & data_type : index_description.data_types)
    {
        DataTypePtr t = data_type;
        if (const auto * arr = typeid_cast<const DataTypeArray *>(t.get()))
            t = arr->getNestedType();
        if (const auto * nullable = typeid_cast<const DataTypeNullable *>(t.get()))
        {
            if (typeid_cast<const DataTypeLowCardinality *>(nullable->getNestedType().get()))
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Projection text index does not support Nullable(LowCardinality(...)). Use LowCardinality(Nullable(...)) instead.");
        }
    }

    static_cast<ProjectionIndexText &>(*result.index).index = std::make_shared<MergeTreeProjectionIndexText>(
        result, std::static_pointer_cast<const MergeTreeIndexText>(textIndexCreator(index_description)));

    result.required_columns = index_description.expression->getRequiredColumns();
    result.with_parent_part_offset = true;
    /// `_part_offset` is the virtual column injected as the projection's doc_id source. It is
    /// not a real column in the parent table, so it must be stripped from `required_columns`
    /// to avoid leaking into MergeTask::extractMergingAndGatheringColumns (which would then
    /// try to resolve it as a subcolumn and throw BAD_ARGUMENTS during ReplacingMergeTree /
    /// SummingMergeTree / lightweight-delete merges). This mirrors the normal-projection
    /// path in ProjectionsDescription.cpp (`_part_offset` → `_parent_part_offset` rename).
    std::erase_if(result.required_columns, [](const String & s) { return s == "_part_offset"; });
    StorageInMemoryMetadata metadata;
    metadata.partition_key = KeyDescription::buildEmptyKey();

    result.type = ProjectionDescription::Type::Aggregate;
    result.sample_block_for_keys.insert({nullptr, std::make_shared<DataTypeString>(), "term"});
    auto posting_list_type = createPostingListType(
        index_ast->as<ASTIndexDeclaration>()->getType()->arguments,
        PostingListParams(index->text_index->params, POSTING_LIST_FORMAT_VERSION_CURRENT, "bitpacking"));
    result.sample_block
        = {result.sample_block_for_keys.getByPosition(0), {posting_list_type->createColumn(), posting_list_type, "posting"}};

    ColumnsDescription projection_columns(result.sample_block.getNamesAndTypesList());
    /// TODO(amos): Add some setting to customize codec
    // projection_columns.modify(
    //     "term", [&](ColumnDescription & column) { column.codec = makeASTFunction("CODEC", std::make_shared<ASTIdentifier>("ZSTD")); });

    // projection_columns.modify(
    //     "posting_list",
    //     [&](ColumnDescription & column) { column.codec = makeASTFunction("CODEC", std::make_shared<ASTIdentifier>("ZSTD")); });

    auto term_ident = make_intrusive<ASTIdentifier>("term");
    metadata.sorting_key = KeyDescription::getKeyFromAST(term_ident, projection_columns, {}, query_context);
    metadata.primary_key = KeyDescription::getKeyFromAST(term_ident, projection_columns, {}, query_context);
    metadata.primary_key.definition_ast = nullptr;
    metadata.setColumns(std::move(projection_columns));

    result.metadata = std::make_shared<StorageInMemoryMetadata>(metadata);
}

std::shared_ptr<MergeTreeSettings> ProjectionIndexText::getDefaultSettings() const
{
    auto settings = std::make_shared<MergeTreeSettings>();

    /// Same default as text index v3
    settings->set("compress_per_column_in_compact_parts", false);
    settings->set("write_marks_for_substreams_in_compact_parts", false);
    /// `index` is populated lazily by `fillProjectionDescription`, which runs AFTER
    /// `getDefaultSettings` (see ProjectionsDescription.cpp). Extract `dictionary_block_size`
    /// directly from the parsed text-index AST so the projection part's `index_granularity`
    /// honours the user's `TYPE text(..., dictionary_block_size = N)` from the start —
    /// otherwise the projection mark layout would silently disagree with the on-disk
    /// posting list metadata produced by the text-index aggregator.
    const auto * decl = index_ast->as<ASTIndexDeclaration>();
    const ASTPtr text_index_arguments = (decl && decl->getType()) ? decl->getType()->arguments : nullptr;
    UInt64 dictionary_block_size = getTextIndexDictionaryBlockSizeFromAST(text_index_arguments);
    settings->set("index_granularity", dictionary_block_size);
    /// TODO(amos): Maybe adaptive is better
    settings->set("index_granularity_bytes", std::numeric_limits<UInt64>::max());

    /// Always use compact part
    settings->set("min_bytes_for_wide_part", std::numeric_limits<UInt64>::max());
    settings->set("min_level_for_wide_part", 0);
    settings->set("min_rows_for_wide_part", 0);

    /// No sparse
    settings->set("ratio_of_defaults_for_sparse_serialization", 1.0);
    settings->set("serialization_info_version", "with_types");
    settings->set("string_serialization_version", "with_size_stream");

    /// Text projections only store 'term' and 'posting' columns, keeping file names short and predictable. Disabling
    /// filename hashing avoids complexity that this projection type does not yet support and prevents potential logical
    /// issues during heavy stress or fuzzy testing.
    settings->set("replace_long_file_name_to_hash", 0);

    /// Projection text index parts have a fixed column set — no virtual columns like _block_number.
    settings->set("enable_block_number_column", false);
    settings->set("enable_block_offset_column", false);

    return settings;
}

const IndexDescription * ProjectionIndexText::getIndexDescription() const
{
    return &index_description;
}

MergeTreeIndexPtr ProjectionIndexText::getIndex() const
{
    /// Build a fresh `MergeTreeProjectionIndexText` (with a fresh inner
    /// `MergeTreeIndexText` carrying its own tokenizer) per call. The
    /// tokenizer holds mutable per-call iterator state (e.g.
    /// `SparseGramsTokenizer::sparse_grams_iterator`) and must not be
    /// shared across concurrent queries. This mirrors how skip text
    /// indexes are obtained in `ReadFromMergeTree::buildIndexes` via
    /// `MergeTreeIndexFactory::instance().get(index)` — a fresh
    /// `MergeTreeIndexText` per call.
    auto fresh_text_index = std::static_pointer_cast<const MergeTreeIndexText>(textIndexCreator(index_description));
    return std::make_shared<MergeTreeProjectionIndexText>(index_description, std::move(fresh_text_index));
}

namespace
{

template <bool enable_phrase, typename Input>
Block tokenize(
    const ITokenizer & extractor,
    const Input & input_data_column,
    UInt32 starting_offset,
    Block sample,
    const IColumn::Offsets * array_offsets,
    const ColumnUInt8::Container * null_map,
    const IColumn * index_column,
    bool lc_dict_has_null,
    const IColumnPermutation * perm_ptr,
    const ColumnUInt8::Container * row_exists_map)
{
    ssize_t rows = array_offsets ? array_offsets->size() : (index_column ? index_column->size() : input_data_column.size());
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
    constexpr size_t base_state_size = sizeof(PostingListData);
    constexpr size_t state_align = alignof(PostingListData);
    static_assert(base_state_size % state_align == 0, "PostingListData size must be multiple of its alignment");
    chassert(posting_states.getAggregateFunction()->sizeOfData() == base_state_size);
    chassert(posting_states.getAggregateFunction()->alignOfData() == state_align);

    /// Allocate context after PostingListData in the same Arena slot.
    using WriteCtx = std::conditional_t<enable_phrase, TokenWriteContextPhrase, TokenWriteContext>;
    constexpr size_t state_size = (base_state_size + sizeof(WriteCtx) + state_align - 1) / state_align * state_align;

    constexpr size_t rows_per_chunk = 8192;
    const size_t fixed_arena_chunk_size = rows_per_chunk * state_size;

    /// Arena for posting lists
    Arena & arena = posting_states.createOrGetArena();
    ArenaPtr posting_arena = std::make_shared<Arena>(fixed_arena_chunk_size, 1, fixed_arena_chunk_size);
    posting_states.addArena(posting_arena);

    /// Local arena scoped to tokenize(): holds PagePool segments, released at end.
    /// PostingListChunk encoded blocks live in posting_arena (must outlive tokenize).
    Arena page_arena(fixed_arena_chunk_size, 1, fixed_arena_chunk_size);
    PagePool page_pool(&page_arena);

    /// Shared scratch buffer for gather+encode, reused across all tokens.
    alignas(16) UInt32 scratch_buffer[TURBOPFOR_BLOCK_SIZE];

    /// Shared TurboPFor encode output buffer. Sized to the worst-case 256-element block;
    /// reused across all tokens' flush/finalize calls (each call consumes it atomically
    /// before the encoded bytes are copied into a PostingListChunk).
    uint8_t packed_buffer[TURBOPFOR_MAX_ENCODED_SIZE];

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
        for (ssize_t sorted_pos = 0; sorted_pos < rows; ++sorted_pos)
        {
            /// When perm_ptr is provided, iterate in sorted order so doc_ids
            /// (starting_offset + sorted_pos) are monotonically increasing.
            /// perm[sorted_pos] gives the physical row in the unsorted block.
            ssize_t physical_row = perm_ptr ? static_cast<ssize_t>((*perm_ptr)[sorted_pos]) : sorted_pos;

            if (row_exists_map && !(*row_exists_map)[physical_row])
                continue;

            size_t begin_row = physical_row;
            size_t end_row = physical_row + 1;

            if (array_offsets)
            {
                begin_row = array_offsets->prevOrZero(physical_row);
                end_row = (*array_offsets)[physical_row];
            }

            [[maybe_unused]] UInt32 token_position = 0;
            for (size_t i = begin_row; i < end_row; ++i)
            {
                if constexpr (has_null && std::is_same_v<Index, void>)
                {
                    /// Direct Nullable(...) wrapper: per-row null map keyed by row index.
                    if ((*null_map)[i])
                        continue;
                }

                if constexpr (std::is_same_v<Index, void>)
                {
                    data = input_data_column.getDataAt(i);
                }
                else
                {
                    UInt64 idx = index->getUInt(i);
                    if constexpr (has_null)
                    {
                        /// LowCardinality(Nullable(...)): the dictionary stores the NULL
                        /// placeholder at position 0 by ClickHouse's `ColumnLowCardinality`
                        /// invariant, so any NULL row has `lowcard_index[i] == 0`. Detect it
                        /// directly off the index — `getDataAt(0)` would otherwise return the
                        /// nested column's `insertDefault()` value (empty `""` for `String`,
                        /// or N NUL bytes for `FixedString(N)`); relying on a downstream
                        /// `data.empty()` check works for `String` but silently lets the NUL
                        /// bytes of `FixedString` flow into tokenizers that treat them as
                        /// content.
                        if (idx == 0)
                            continue;
                    }
                    data = input_data_column.getDataAt(idx);
                }

                if (data.empty())
                    continue;

                forEachToken(
                    extractor,
                    data.data(),
                    data.size(),
                    [&](const char * token_start, size_t token_length)
                    {
                        std::string_view term(token_start, token_length);
                        bool inserted = false;
                        terms_hashing.emplace(term, it, inserted);

                        if (inserted)
                        {
                            char * current_place = allocate_place();
                            new (current_place) PostingListData(PostingListKind::Writer);
                            new (current_place + base_state_size) WriteCtx();
                            it->getMapped() = current_place;
                            terms_bytes += token_length;
                        }

                        UInt32 doc_id = static_cast<UInt32>(starting_offset + sorted_pos);

                        {
                            auto * ctx = reinterpret_cast<WriteCtx *>(it->getMapped() + base_state_size);
                            if constexpr (enable_phrase)
                                ctx->add(doc_id, token_position, page_pool, *posting_arena, scratch_buffer, packed_buffer);
                            else
                                ctx->add(doc_id, page_pool, *posting_arena, scratch_buffer, packed_buffer);
                        }

                        if (unlikely(token_position == std::numeric_limits<UInt32>::max()))
                            throw Exception(
                                ErrorCodes::LIMIT_EXCEEDED,
                                "Text index token position overflow: document has more than {} tokens",
                                std::numeric_limits<UInt32>::max());
                        ++token_position;
                        return false;
                    });
            }
        }
    };

    /// Dispatch decodes two independent signals into one `has_null` template flag:
    ///   * `null_map != nullptr`              — direct `Nullable(...)` wrapper, null map by row index
    ///   * `lc_dict_has_null && index_column` — `LowCardinality(Nullable(...))`, null detected via
    ///                                          `lowcard_index[i] == 0`
    /// Both routes share the same `has_null` instantiation; the inner loop branches on
    /// `Index == void` to pick the correct null-detection path.
    if (null_map && !index_column)
        work.template operator()<true, void>(nullptr);
    else if (!null_map && !index_column)
        work.template operator()<false, void>(nullptr);
    else if (lc_dict_has_null)
    {
        if (const auto * uint8 = checkAndGetColumn<ColumnUInt8>(index_column))
            work.template operator()<true>(uint8);
        else if (const auto * uint16 = checkAndGetColumn<ColumnUInt16>(index_column))
            work.template operator()<true>(uint16);
        else if (const auto * uint32 = checkAndGetColumn<ColumnUInt32>(index_column))
            work.template operator()<true>(uint32);
        else if (const auto * uint64 = checkAndGetColumn<ColumnUInt64>(index_column))
            work.template operator()<true>(uint64);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected size of index type for low cardinality column");
    }
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
        /// Finalize all TokenWriteContexts → populate PostingListWriter state
        terms_hashing.forEachValue(
            [&](const auto & /* key */, auto & mapped)
            {
                auto * posting_data = reinterpret_cast<PostingListData *>(mapped);
                chassert(posting_data->isWriter());
                auto * ctx = reinterpret_cast<WriteCtx *>(mapped + base_state_size);
                ctx->finalize(page_pool, *posting_arena, scratch_buffer, packed_buffer);
            });

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
    const IColumnPermutation * perm_ptr) const
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

    if (starting_offset > std::numeric_limits<UInt32>::max() || starting_offset + block.rows() > std::numeric_limits<UInt32>::max())
    {
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "Text index doc ID overflow: starting_offset={} + rows={} exceeds UInt32 range",
            starting_offset,
            block.rows());
    }

    /// Skip rows deleted by lightweight delete mutations.
    const ColumnUInt8::Container * row_exists_map = nullptr;
    if (block.has(RowExistsColumn::name))
        row_exists_map = &assert_cast<const ColumnUInt8 &>(*block.getByName(RowExistsColumn::name).column).getData();

    Block working_block = block;
    if (!working_block.has(index_description.column_names[0]))
    {
        for (const auto & required_column : index_description.expression->getRequiredColumns())
            if (!working_block.has(required_column))
                working_block.insert(working_block.getSubcolumnByName(required_column));
        index_description.expression->execute(working_block);
    }
    const auto & index_column = working_block.getByName(index_description.column_names[0]);
    ColumnPtr doc_column = index_column.column;
    auto [processed_column, _] = index->text_index->preprocessor->processColumn(index_column, 0, index_column.column->size());
    doc_column = processed_column;

    const IColumn::Offsets * array_offsets = nullptr;
    const ColumnUInt8::Container * null_map = nullptr;
    const IColumn * lowcard_index = nullptr;
    /// Set when the column is `LowCardinality(Nullable(...))`: signals the tokenize loop
    /// to skip rows whose LowCardinality index is 0 (the dictionary's NULL placeholder).
    /// We do not synthesise a row-level null map for this case — the `idx == 0` check is
    /// O(1) per row and matches the existing branch budget; see the `work` lambda below.
    bool lc_dict_has_null = false;

    if (const auto * array = checkAndGetColumn<ColumnArray>(doc_column.get()))
    {
        array_offsets = &array->getOffsets();
        doc_column = array->getDataPtr();
    }

    /// Mirrors `MergeTreeIndexText::getNestedDataType` (validation) — unwrap the same set of
    /// wrappers in the same order so any column shape that validation accepted is also handled
    /// here. Three combinations are supported:
    ///   * `Nullable(String/FixedString)` — outer Nullable, per-row null map
    ///   * `LowCardinality(String/FixedString)` — LC indexes into a plain dictionary
    ///   * `LowCardinality(Nullable(String/FixedString))` — LC indexes into a nullable
    ///     dictionary; the tokenize loop detects NULL rows via `lowcard_index[i] == 0`
    ///     (ClickHouse's `ColumnLowCardinality` invariant places the NULL placeholder at
    ///     dictionary position 0).
    /// `Nullable(LowCardinality(...))` is rejected globally by ClickHouse's type construction
    /// rules — `Nullable` cannot wrap `LowCardinality`; the check below is defence-in-depth
    /// and never fires under normal use.
    if (const auto * nullable = checkAndGetColumn<ColumnNullable>(doc_column.get()))
    {
        null_map = &nullable->getNullMapData();
        auto nested = nullable->getNestedColumnPtr();
        if (checkAndGetColumn<ColumnLowCardinality>(nested.get()))
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Projection text index does not support Nullable(LowCardinality(...)). Use LowCardinality(Nullable(...)) instead.");
        doc_column = nested;
    }
    else if (const auto * low_card = checkAndGetColumn<ColumnLowCardinality>(doc_column.get()))
    {
        lowcard_index = &low_card->getIndexes();
        const auto & dict = low_card->getDictionary().getNestedColumn();
        if (const auto * dict_nullable = checkAndGetColumn<ColumnNullable>(dict.get()))
        {
            lc_dict_has_null = true;
            doc_column = dict_nullable->getNestedColumnPtr();
        }
        else
        {
            doc_column = dict;
        }
    }

    const auto * column_string = checkAndGetColumn<ColumnString>(doc_column.get());
    const auto * column_fixed_string = checkAndGetColumn<ColumnFixedString>(doc_column.get());
    if (!column_string && !column_fixed_string)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Inverted index projection only accepts string columns for now");

    Block tokenized_block;
    bool phrase_support = index->text_index->params.enable_phrase_query_support;

    auto do_tokenize = [&]<bool phrase>(const auto & col) -> Block
    {
        return tokenize<phrase>(
            *index->text_index->tokenizer,
            col,
            static_cast<UInt32>(starting_offset),
            projection_desc.sample_block,
            array_offsets,
            null_map,
            lowcard_index,
            lc_dict_has_null,
            perm_ptr,
            row_exists_map);
    };

    if (column_string)
    {
        tokenized_block = phrase_support ? do_tokenize.template operator()<true>(*column_string)
                                         : do_tokenize.template operator()<false>(*column_string);
    }
    else
    {
        tokenized_block = phrase_support ? do_tokenize.template operator()<true>(*column_fixed_string)
                                         : do_tokenize.template operator()<false>(*column_fixed_string);
    }

    return tokenized_block;
}

}
