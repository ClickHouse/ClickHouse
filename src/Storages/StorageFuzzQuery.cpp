#include <Storages/StorageFuzzQuery.h>

#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/StorageFactory.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <QueryPipeline/Pipe.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

ColumnPtr FuzzQuerySource::createColumn()
{
    auto column = ColumnString::create();
    ColumnString::Chars & data_to = column->getChars();
    ColumnString::Offsets & offsets_to = column->getOffsets();

    offsets_to.resize(block_size);
    IColumn::Offset offset = 0;

    auto fuzz_base = query;
    size_t row_num = 0;

    /// Bound retries per row so the loop cannot spin forever. Even with the precise
    /// `if (fuzzed_text.size() > config.max_query_length)` check below the loop can spin
    /// if the fuzzer keeps producing oversized variants (e.g. a small cap like 1, where
    /// every non-trivial mutation overflows). Fall back to emitting the original
    /// (unfuzzed) query so the source always makes progress.
    constexpr size_t max_attempts_per_row = 100;
    size_t attempts_for_current_row = 0;

    while (row_num < block_size)
    {
        if (isCancelled())
            break;

        if (attempts_for_current_row >= max_attempts_per_row)
        {
            auto fallback_text = query->formatForErrorMessage();
            IColumn::Offset next_offset = offset + fallback_text.size();
            data_to.resize(next_offset);
            std::copy(fallback_text.begin(), fallback_text.end(), &data_to[offset]);
            offsets_to[row_num] = next_offset;
            offset = next_offset;
            fuzz_base = query;
            ++row_num;
            attempts_for_current_row = 0;
            continue;
        }

        ++attempts_for_current_row;

        ASTPtr new_query = fuzz_base->clone();

        auto base_before_fuzz = fuzz_base->formatForErrorMessage();
        fuzzer.fuzzMain(new_query);
        auto fuzzed_text = new_query->formatForErrorMessage();

        if (base_before_fuzz == fuzzed_text)
            continue;

        /// Fuzzed AST exceeds the configured cap. Reset to the original query and try
        /// again. The previous lazy guard `config.max_query_length > 500` always reset
        /// when the cap was large, regardless of the actual fuzzed size — so the loop
        /// never produced a row (the original hang). This precise check resets only when
        /// the fuzzed text really exceeds the cap, avoiding `O(max_attempts_per_row *
        /// block_size)` wasted fuzz/format work for the common `cap > 500` case.
        if (fuzzed_text.size() > config.max_query_length)
        {
            fuzz_base = query;
            continue;
        }

        IColumn::Offset next_offset = offset + fuzzed_text.size();
        data_to.resize(next_offset);

        std::copy(fuzzed_text.begin(), fuzzed_text.end(), &data_to[offset]);

        offsets_to[row_num] = next_offset;

        offset = next_offset;
        fuzz_base = new_query;
        ++row_num;
        attempts_for_current_row = 0;
    }

    /// On cancellation we may have produced fewer rows than `block_size`. Shrink the
    /// offsets to the actual count instead of backfilling with the unfuzzed query: the
    /// backfill would do `O(block_size * query_length)` extra work after `KILL QUERY` /
    /// `max_execution_time` cancellation, defeating fast cancel response. The caller
    /// (`generate`) checks `isCancelled` and discards the partial column.
    if (row_num < block_size)
        offsets_to.resize(row_num);

    return column;
}

StorageFuzzQuery::StorageFuzzQuery(
    const StorageID & table_id_, const ColumnsDescription & columns_, const String & comment_, const Configuration & config_)
    : StorageWithCommonVirtualColumns(table_id_), config(config_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setComment(comment_);
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}

VirtualColumnsDescription StorageFuzzQuery::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    return desc;
}

Pipe StorageFuzzQuery::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    storage_snapshot->check(column_names);

    Pipes pipes;
    pipes.reserve(num_streams);

    const ColumnsDescription & our_columns = storage_snapshot->metadata->getColumns();
    Block block_header;
    for (const auto & name : column_names)
    {
        const auto & name_type = our_columns.get(name);
        MutableColumnPtr column = name_type.type->createColumn();
        block_header.insert({std::move(column), name_type.type, name_type.name});
    }

    const char * begin = config.query.data();
    const char * end = begin + config.query.size();

    ParserQuery parser(end, false);
    auto query = parseQuery(parser, begin, end, "", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

    for (UInt64 i = 0; i < num_streams; ++i)
        pipes.emplace_back(std::make_shared<FuzzQuerySource>(max_block_size, std::make_shared<const Block>(block_header), config, query));

    return Pipe::unitePipes(std::move(pipes));
}

StorageFuzzQuery::Configuration StorageFuzzQuery::getConfiguration(ASTs & engine_args, ContextPtr local_context)
{
    StorageFuzzQuery::Configuration configuration{};

    // Supported signatures:
    //
    // FuzzQuery(query)
    // FuzzQuery(query, max_query_length)
    // FuzzQuery(query, max_query_length, random_seed)
    if (engine_args.empty() || engine_args.size() > 3)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "FuzzQuery requires 1 to 3 arguments: query, max_query_length, random_seed");

    for (auto & engine_arg : engine_args)
        engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, local_context);

    auto first_arg = checkAndGetLiteralArgument<String>(engine_args[0], "query");
    configuration.query = std::move(first_arg);

    if (engine_args.size() >= 2)
    {
        const auto & literal = engine_args[1]->as<const ASTLiteral &>();
        if (!literal.value.isNull())
            configuration.max_query_length = checkAndGetLiteralArgument<UInt64>(literal, "max_query_length");
    }

    if (engine_args.size() == 3)
    {
        const auto & literal = engine_args[2]->as<const ASTLiteral &>();
        if (!literal.value.isNull())
            configuration.random_seed = checkAndGetLiteralArgument<UInt64>(literal, "random_seed");
    }

    /// `max_query_length == 0` is pathological: every non-empty fuzzed AST exceeds the
    /// cap, the loop in `FuzzQuerySource::createColumn` resets and only ever falls back
    /// to the unfuzzed query (after `max_attempts_per_row` retries per row). Reject it
    /// up front rather than degrading to a no-op fuzzer.
    if (configuration.max_query_length == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "FuzzQuery `max_query_length` must be greater than 0");

    return configuration;
}

void registerStorageFuzzQuery(StorageFactory & factory)
{
    factory.registerStorage(
        "FuzzQuery",
        [](const StorageFactory::Arguments & args) -> std::shared_ptr<StorageFuzzQuery>
        {
            ASTs & engine_args = args.engine_args;

            if (engine_args.empty())
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Storage FuzzQuery must have arguments.");

            StorageFuzzQuery::Configuration configuration = StorageFuzzQuery::getConfiguration(engine_args, args.getLocalContext());

            for (const auto& col : args.columns)
                if (col.type->getTypeId() != TypeIndex::String)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "'StorageFuzzQuery' supports only columns of String type, got {}.", col.type->getName());

            return std::make_shared<StorageFuzzQuery>(args.table_id, args.columns, args.comment, configuration);
        });
}

}
