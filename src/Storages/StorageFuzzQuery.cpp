#include <Storages/StorageFuzzQuery.h>

#include <optional>
#include <string_view>
#include <unordered_set>
#include <Columns/ColumnString.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/StorageFactory.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Parsers/formatAST.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_DATA;
}

ColumnPtr FuzzQuerySource::createColumn()
{
    auto column = ColumnString::create();
    ColumnString::Chars & data_to = column->getChars();
    ColumnString::Offsets & offsets_to = column->getOffsets();

    offsets_to.resize(block_size);
    IColumn::Offset offset = 0;

    for (size_t row_num = 0; row_num < block_size; ++row_num)
    {
        ASTPtr new_query = query->clone();
        fuzzer.fuzzMain(new_query);

        WriteBufferFromOwnString out;
        formatAST(*new_query, out, false);
        auto data = out.str();
        size_t data_len = data.size();

        IColumn::Offset next_offset = offset + data_len + 1;
        data_to.resize(next_offset);

        std::copy(data.begin(), data.end(), &data_to[offset]);

        data_to[offset + data_len] = 0;
        offsets_to[row_num] = next_offset;

        offset = next_offset;
    }

    return column;
}

StorageFuzzQuery::StorageFuzzQuery(
    const StorageID & table_id_, const ColumnsDescription & columns_, const String & comment_, const Configuration & config_)
    : IStorage(table_id_), config(config_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setComment(comment_);
    setInMemoryMetadata(storage_metadata);
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

    ParserQuery parser(end, 0);
    auto query = parseQuery(parser, begin, end, "", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

    for (UInt64 i = 0; i < num_streams; ++i)
        pipes.emplace_back(std::make_shared<FuzzQuerySource>(max_block_size, block_header, config, query));

    return Pipe::unitePipes(std::move(pipes));
}

static constexpr std::array<std::string_view, 2> optional_configuration_keys = {"query_str", "random_seed"};

void StorageFuzzQuery::processNamedCollectionResult(Configuration & configuration, const NamedCollection & collection)
{
    validateNamedCollection(
        collection,
        std::unordered_set<std::string>(),
        std::unordered_set<std::string>(optional_configuration_keys.begin(), optional_configuration_keys.end()));

    if (collection.has("query"))
        configuration.query = collection.get<String>("query");

    if (collection.has("random_seed"))
        configuration.random_seed = collection.get<UInt64>("random_seed");
}

StorageFuzzQuery::Configuration StorageFuzzQuery::getConfiguration(ASTs & engine_args, ContextPtr local_context)
{
    StorageFuzzQuery::Configuration configuration{};

    if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, local_context))
    {
        StorageFuzzQuery::processNamedCollectionResult(configuration, *named_collection);
    }
    else
    {
        // Supported signatures:
        //
        // FuzzQuery('query')
        // FuzzQuery('query', 'random_seed')
        if (engine_args.empty() || engine_args.size() > 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "FuzzQuery requires 1 to 2 arguments: query, random_seed");

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, local_context);

        auto first_arg = checkAndGetLiteralArgument<String>(engine_args[0], "query");
        configuration.query = std::move(first_arg);

        if (engine_args.size() == 2)
        {
            const auto & literal = engine_args[1]->as<const ASTLiteral &>();
            if (!literal.value.isNull())
                configuration.random_seed = checkAndGetLiteralArgument<UInt64>(literal, "random_seed");
        }
    }
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
