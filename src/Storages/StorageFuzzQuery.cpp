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
    auto base_text = fuzz_base->formatForErrorMessage();
    size_t row_num = 0;

    /// Per-row attempt budget: when fuzzing keeps producing oversized variants we fall back
    /// to emitting the original (unfuzzed) query so the loop always makes progress. The
    /// formatted base query is guaranteed to fit `max_query_length` because
    /// `getConfiguration` rejects configurations where it does not.
    constexpr size_t max_attempts_per_row = 1024;

    while (row_num < block_size)
    {
        /// Stop generating rows promptly on cancellation (e.g. `KILL QUERY` or
        /// `max_execution_time`) instead of filling the rest of the block with fallback rows.
        if (isCancelled())
            break;

        String text_to_emit;
        bool produced = false;

        for (size_t attempt = 0; attempt < max_attempts_per_row; ++attempt)
        {
            ASTPtr new_query = fuzz_base->clone();

            auto base_before_fuzz = fuzz_base->formatForErrorMessage();
            fuzzer.fuzzMain(new_query);
            auto fuzzed_text = new_query->formatForErrorMessage();

            if (base_before_fuzz == fuzzed_text)
                continue;

            /// AST is too long, will start from the original query.
            if (fuzzed_text.size() > config.max_query_length)
            {
                fuzz_base = query;
                continue;
            }

            text_to_emit = std::move(fuzzed_text);
            fuzz_base = new_query;
            produced = true;
            break;
        }

        if (!produced)
        {
            /// Fallback: emit the formatted unfuzzed query. Always within `max_query_length`
            /// thanks to the upfront check in `getConfiguration`.
            text_to_emit = base_text;
            fuzz_base = query;
        }

        IColumn::Offset next_offset = offset + text_to_emit.size();
        data_to.resize(next_offset);

        std::copy(text_to_emit.begin(), text_to_emit.end(), &data_to[offset]);

        offsets_to[row_num] = next_offset;

        offset = next_offset;
        ++row_num;
    }

    /// Early break on cancellation may leave the tail of `offsets_to` uninitialised;
    /// shrink to the rows actually produced.
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

    /// `max_query_length == 0` would make `FuzzQuerySource::createColumn` loop forever:
    /// every non-empty fuzzed AST exceeds the cap, the loop resets and never produces a row.
    if (configuration.max_query_length == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "FuzzQuery `max_query_length` must be greater than 0");

    /// The fuzzed text is bounded by `max_query_length`; if even the formatted base query
    /// exceeds the cap, no fuzzed variant can satisfy it and the source would have to
    /// silently emit oversized fallback rows. Reject the impossible configuration up front
    /// so the contract `length(generated) <= max_query_length` holds for every emitted row.
    const char * begin = configuration.query.data();
    const char * end = begin + configuration.query.size();
    ParserQuery parser(end, false);
    auto base_ast = parseQuery(parser, begin, end, "FuzzQuery base query", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    const size_t base_formatted_size = base_ast->formatForErrorMessage().size();
    if (base_formatted_size > configuration.max_query_length)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "FuzzQuery `max_query_length` ({}) is smaller than the formatted base query length ({}); "
            "fuzzer cannot produce any rows that satisfy the cap",
            configuration.max_query_length, base_formatted_size);

    return configuration;
}

void registerStorageFuzzQuery(StorageFactory & factory);
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
