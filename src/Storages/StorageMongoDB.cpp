#include "config.h"

#if USE_MONGODB
#include <memory>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <IO/Operators.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Sources/MongoDBSource.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMongoDB.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Common/ErrorCodes.h>
#include <Common/parseAddress.h>

#include <mongocxx/instance.hpp>

#include <bsoncxx/json.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/array.hpp>

using bsoncxx::builder::basic::document;
using bsoncxx::builder::basic::array;
using bsoncxx::builder::basic::make_document;
using bsoncxx::builder::basic::kvp;

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int MONGODB_CANNOT_AUTHENTICATE;
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

mongocxx::instance inst{};

StorageMongoDB::StorageMongoDB(
    const StorageID & table_id_,
    const std::string & host_,
    uint16_t port_,
    const std::string & database_name_,
    const std::string & collection_name_,
    const std::string & username_,
    const std::string & password_,
    const std::string & options_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment)
    : IStorage{table_id_}
    , database_name{database_name_}
    , collection_name{collection_name_}
    , uri{"mongodb://" + username_ + ":" + password_ + "@" + host_ + ":" + toString(port_) + "/" + database_name_ + "?" + options_}
    , log(getLogger("StorageMongoDB (" + table_id_.table_name + ")"))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageMongoDB::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);

    Block sample_block;
    for (const String & column_name : column_names)
    {
        auto column_data = storage_snapshot->metadata->getColumns().getPhysical(column_name);
        sample_block.insert({ column_data.type, column_data.name });
    }

    auto options = mongocxx::options::find();

    return Pipe(std::make_shared<MongoDBSource>(uri, database_name, collection_name, createMongoDBQuery(&options, &query_info),
                                                std::move(options), sample_block, max_block_size));
}

SinkToStoragePtr StorageMongoDB::write(const ASTPtr & /* query */, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr /* context */, bool /*async_insert*/)
{
    return nullptr; // TODO: implement
}

StorageMongoDB::Configuration StorageMongoDB::getConfiguration(ASTs engine_args, ContextPtr context)
{
    Configuration configuration;

    if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, context))
    {
        validateNamedCollection(
            *named_collection,
            ValidateKeysMultiset<MongoDBEqualKeysSet>{"host", "port", "user", "username", "password", "database", "db", "collection", "table"},
            {"options"});

        configuration.host = named_collection->getAny<String>({"host", "hostname"});
        configuration.port = static_cast<UInt16>(named_collection->get<UInt64>("port"));
        configuration.username = named_collection->getAny<String>({"user", "username"});
        configuration.password = named_collection->get<String>("password");
        configuration.database = named_collection->getAny<String>({"database", "db"});
        configuration.table = named_collection->getAny<String>({"collection", "table"});
        configuration.options = named_collection->getOrDefault<String>("options", "");
    }
    else
    {
        if (engine_args.size() < 5 || engine_args.size() > 6)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Storage MongoDB requires from 5 to 6 parameters: "
                            "MongoDB('host:port', database, collection, 'user', 'password' [, 'options']).");

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, context);

        /// 27017 is the default MongoDB port.
        auto parsed_host_port = parseAddress(checkAndGetLiteralArgument<String>(engine_args[0], "host:port"), 27017);

        configuration.host = parsed_host_port.first;
        configuration.port = parsed_host_port.second;
        configuration.database = checkAndGetLiteralArgument<String>(engine_args[1], "database");
        configuration.table = checkAndGetLiteralArgument<String>(engine_args[2], "table");
        configuration.username = checkAndGetLiteralArgument<String>(engine_args[3], "username");
        configuration.password = checkAndGetLiteralArgument<String>(engine_args[4], "password");

        if (engine_args.size() >= 6)
            configuration.options = checkAndGetLiteralArgument<String>(engine_args[5], "database");
    }

    context->getRemoteHostFilter().checkHostAndPort(configuration.host, toString(configuration.port));

    return configuration;
}

String StorageMongoDB::getFuncName(const String & func)
{
    if (func == "equals")
        return "$eq";
    if (func == "greaterThan")
        return "$gt";
    if (func == "greaterOrEquals")
        return "$gte";
    if (func == "in")
        return "$in";
    if (func == "lessThan")
        return "$lt";
    if (func == "lessOrEquals")
        return "$lte";
    if (func == "notEquals")
        return "$ne";
    if (func == "notIn")
        return "$ne";
    if (func == "and")
        return "$and";
    if (func == "or")
        return "$or";

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "function '{}' is not supported", func);
}

bsoncxx::types::bson_value::value StorageMongoDB::toBSONValue(const Field * field)
{
    switch(field->getType())
    {
        case Field::Types::Null:
            return bsoncxx::types::b_null();
        case Field::Types::UInt64:
            return static_cast<Int64>(field->get<UInt64 &>());
        case Field::Types::Int64:
            return field->get<Int64 &>();
        case Field::Types::Float64:
            return field->get<Float64 &>();
        case Field::Types::String:
            return field->get<String &>();
        case Field::Types::Array:
        {
            auto arr = array();
            for (const auto & tuple_field : field->get<Array &>())
                arr.append(toBSONValue(&tuple_field));
            return arr.view();
        }
        case Field::Types::Tuple:
        {
            auto arr =array();
            for (const auto & tuple_field : field->get<Tuple &>())
                arr.append(toBSONValue(&tuple_field));
            return arr.view();
        }
        case Field::Types::Map:
        {
            auto doc = document();
            for (const auto & element : field->get<Map &>())
            {
                const auto & tuple = element.get<Tuple &>();
                doc.append(kvp(tuple.at(0).get<String &>(), toBSONValue(&tuple.at(1))));
            }
            return doc.view();
        }
        case Field::Types::UUID:
            return static_cast<String>(formatUUID(field->get<UUID &>()));
        case Field::Types::Bool:
            return static_cast<bool>(field->get<bool &>());
        case Field::Types::Object:
        {
            auto doc = document();
            for (const auto & [key, var] : field->get<Object &>())
                doc.append(kvp(key, toBSONValue(&var)));
            return doc.view();
        }
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "field's type '{}' is not supported", field->getTypeName());
    }
}

bsoncxx::document::value StorageMongoDB::visitFunction(const ASTFunction * func)
{
    const auto & func_name = getFuncName(func->name);
    if (const auto & explist = func->children.at(0)->as<ASTExpressionList>())
    {
        if (const auto & identifier = explist->children.at(0)->as<ASTIdentifier>())
        {
            const auto & expression = explist->children.at(1);
            if (const auto & literal = expression->as<ASTLiteral>())
                return make_document(kvp(identifier->shortName(), make_document(kvp(func_name, toBSONValue(&literal->value)))));
            if (const auto & child_func = expression->as<ASTFunction>())
                return make_document(kvp(identifier->shortName(), make_document(kvp(func_name, visitFunction(child_func)))));

            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "error during parsing the AST: the Function must have an ExpressionList or a Function as second argument, got '{}' instead",
                expression->formatForErrorMessage());
        }


        auto arr = array();
        for (const auto & child : explist->children)
        {
            if (const auto & child_func = child->as<ASTFunction>())
                arr.append(visitFunction(child_func));
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "error during parsing the AST: expected a function in the ExpressionList, got '{}' instead",
                    child->formatForErrorMessage());
        }
        return make_document(kvp(func_name, std::move(arr)));
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR,
        "error during parsing the AST: first child must be an ExpressionList, got '{}' instead", func->children.at(0)->formatForErrorMessage());
}

bsoncxx::document::value StorageMongoDB::createMongoDBQuery(mongocxx::options::find * options, SelectQueryInfo * query)
{
    auto & query_tree = query->query_tree->as<QueryNode &>();

    if (query_tree.hasLimit())
        options->limit(query->limit);
    if (query_tree.hasLimitBy()) 
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,"LIMIT BY is not supported.");
    if (query_tree.hasOffset())
        options->skip(query_tree.getOffset()->as<ConstantNode &>().getValue().safeGet<UInt64>());
    if (query_tree.hasWindow())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,"WINDOW is not supported.");

    // TODO: _CAST support
    // TODO: projections
    // TODO: sort
    // TODO: aggregation functions

    if (query_tree.hasWhere())
    {
        auto filter = visitFunction( query_tree.getWhere()->toAST()->as<ASTFunction>());
        LOG_INFO(log, "MongoDB query has built: '{}'", bsoncxx::to_json(filter));
        return filter;
    }

    return make_document();
}


void registerStorageMongoDB(StorageFactory & factory)
{
    factory.registerStorage("MongoDB", [](const StorageFactory::Arguments & args)
    {
        auto configuration = StorageMongoDB::getConfiguration(args.engine_args, args.getLocalContext());

        return std::make_shared<StorageMongoDB>(
            args.table_id,
            configuration.host,
            configuration.port,
            configuration.database,
            configuration.table,
            configuration.username,
            configuration.password,
            configuration.options,
            args.columns,
            args.constraints,
            args.comment);
    },
    {
        .source_access_type = AccessType::MONGO,
    });
}

}
#endif
