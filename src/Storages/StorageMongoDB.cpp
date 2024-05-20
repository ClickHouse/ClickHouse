#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/SortNode.h>
#include <Databases/DatabaseFactory.h>
#include <Formats/BSONTypes.h>


#include "config.h"

#if USE_MONGODB
#include <memory>

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
#include <Common/parseAddress.h>

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
    extern const int NOT_IMPLEMENTED;
    extern const int TYPE_MISMATCH;
}

StorageMongoDB::StorageMongoDB(
    const StorageID & table_id_,
    MongoDBConfiguration configuration_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment)
    : IStorage{table_id_}
    , configuration{std::move(configuration_)}
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

    return Pipe(std::make_shared<MongoDBSource>(*configuration.uri, configuration.collection, buildMongoDBQuery(&options, &query_info, sample_block),
        std::move(options), sample_block, max_block_size));
}

MongoDBConfiguration StorageMongoDB::getConfiguration(ASTs engine_args, ContextPtr context)
{
    MongoDBConfiguration configuration;
    if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, context))
    {
        if (named_collection->has("uri"))
        {
            validateNamedCollection(*named_collection, {"collection"}, {});
            configuration.uri = std::make_unique<mongocxx::uri>(named_collection->get<String>("uri"));
        }
        else
        {
            validateNamedCollection(*named_collection, {"host", "port", "user", "password", "database", "collection"}, {"options"});
            String user = named_collection->get<String>("user");
            String auth_string;
            if (!user.empty())
                auth_string = fmt::format("{}:{}@", user, named_collection->get<String>("password"));
            configuration.uri = std::make_unique<mongocxx::uri>(fmt::format("mongodb://{}{}:{}/{}?{}",
                                                          auth_string,
                                                          named_collection->get<String>("host"),
                                                          named_collection->get<String>("port"),
                                                          named_collection->get<String>("database"),
                                                          named_collection->getOrDefault<String>("options", "")));
        }
        configuration.collection = named_collection->get<String>("collection");
    }
    else
    {
        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, context);

        if (engine_args.size() == 5 || engine_args.size() == 6)
        {
            configuration.collection = checkAndGetLiteralArgument<String>(engine_args[2], "collection");

            String options;
            if (engine_args.size() == 6)
                options = checkAndGetLiteralArgument<String>(engine_args[5], "options");

            String user = checkAndGetLiteralArgument<String>(engine_args[3], "user");
            String auth_string;
            if (!user.empty())
                auth_string = fmt::format("{}:{}@", user, checkAndGetLiteralArgument<String>(engine_args[4], "password"));
            auto parsed_host_port = parseAddress(checkAndGetLiteralArgument<String>(engine_args[0], "host:port"), 27017);
            configuration.uri = std::make_unique<mongocxx::uri>(fmt::format("mongodb://{}{}:{}/{}?{}",
                                                              auth_string,
                                                              parsed_host_port.first,
                                                              parsed_host_port.second,
                                                              checkAndGetLiteralArgument<String>(engine_args[1], "database"),
                                                              options));
        }
        else if (engine_args.size() == 2)
        {
            configuration.collection = checkAndGetLiteralArgument<String>(engine_args[1], "database");
            configuration.uri =  std::make_unique<mongocxx::uri>(checkAndGetLiteralArgument<String>(engine_args[0], "host"));
        }
        else
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                                "Storage MongoDB requires 2 or from to 5 to 6 parameters: "
                                "MongoDB('host:port', 'database', 'collection', 'user', 'password' [, 'options']) or MongoDB('uri', 'collection').");
    }

    configuration.checkHosts(context);

    return configuration;
}

String StorageMongoDB::getMongoFuncName(const String & func)
{
    if (func == "equals" || func == "isNull")
        return "$eq";
    if (func == "notEquals" || func == "isNotNull")
        return "$ne";
    if (func == "greaterThan")
        return "$gt";
    if (func == "lessThan")
        return "$lt";
    if (func == "greaterOrEquals")
        return "$gte";
    if (func == "lessOrEquals")
        return "$lte";
    if (func == "in")
        return "$in";
    if (func == "notIn")
        return "$nin";
    if (func == "lessThan")
        return "$lt";
    if (func == "and")
        return "$and";
    if (func == "or")
        return "$or";

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function '{}' is not supported", func);
}

bsoncxx::types::bson_value::value StorageMongoDB::toBSONValue(const Field * field)
{
    switch (field->getType())
    {
        case Field::Types::Null:
            return bsoncxx::types::b_null();
        case Field::Types::Int64:
            return field->get<Int64 &>();
        case Field::Types::UInt64:
            return static_cast<Int64>(field->get<UInt64 &>());
        case Field::Types::Int128:
            return static_cast<Int64>(field->get<Int128 &>());
        case Field::Types::UInt128:
            return static_cast<Int64>(field->get<UInt128 &>());
        case Field::Types::Int256:
            return static_cast<Int64>(field->get<Int256 &>());
        case Field::Types::UInt256:
            return static_cast<Int64>(field->get<UInt256 &>());
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
            auto arr = array();
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
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Field's type '{}' is not supported", field->getTypeName());
    }
}

bsoncxx::document::value StorageMongoDB::visitWhereFunction(const ASTFunction * func)
{
    const auto & func_name = getMongoFuncName(func->name);
    if (const auto & explist = func->children.at(0)->as<ASTExpressionList>())
    {
        if (const auto & identifier = explist->children.at(0)->as<ASTIdentifier>())
        {
            if (explist->children.size() < 2)
            {
                if (identifier->shortName() == "_id")
                    throw Exception(ErrorCodes::TYPE_MISMATCH, "oid can't be null");
                return make_document(kvp(identifier->shortName(), make_document(kvp(func_name, bsoncxx::types::b_null{}))));
            }
            const auto & expression = explist->children.at(1);
            if (const auto & literal = expression->as<ASTLiteral>())
            {
                if (identifier->shortName() == "_id")
                {
                    if (literal->value.getType() != Field::Types::String)
                        throw Exception(ErrorCodes::TYPE_MISMATCH, "oid can be converted to String only, got type '{}'", literal->value.getTypeName());
                    return make_document(kvp(identifier->shortName(), make_document(kvp(func_name, bsoncxx::oid{literal->value.get<String &>()}))));
                }
                return make_document(kvp(identifier->shortName(), make_document(kvp(func_name, toBSONValue(&literal->value)))));
            }
            if (const auto & child_func = expression->as<ASTFunction>())
            {
                if (child_func->name == "_CAST")
                {
                    const auto & literal = child_func->children.at(0)->as<ASTExpressionList>()->children.at(0)->as<ASTLiteral>();
                    if (identifier->shortName() == "_id")
                    {
                        if (literal->value.getType() != Field::Types::String)
                            throw Exception(ErrorCodes::TYPE_MISMATCH, "oid can be converted to String only, got type '{}'", literal->value.getTypeName());
                        return make_document(kvp(identifier->shortName(), make_document(kvp(func_name, bsoncxx::oid{literal->value.get<String &>()}))));
                    }
                    return make_document(kvp(identifier->shortName(), make_document(kvp(func_name, visitWhereFunction(child_func)))));
                }
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only constant expressions are supported in WHERE section");
            }
        }

        auto arr = array();
        for (const auto & child : explist->children)
        {
            if (const auto & child_func = child->as<ASTFunction>())
                arr.append(visitWhereFunction(child_func));
            else
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only constant expressions are supported in WHERE section");
        }
        return make_document(kvp(func_name, std::move(arr)));
    }
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only constant expressions are supported in WHERE section");
}

bsoncxx::document::value StorageMongoDB::buildMongoDBQuery(mongocxx::options::find * options, SelectQueryInfo * query, const Block & sample_block)
{
    auto & query_tree = query->query_tree->as<QueryNode &>();

    if (query_tree.hasHaving())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "HAVING section is not supported");
    if (query_tree.hasGroupBy())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "GROUP BY section is not supported");
    if (query_tree.hasWindow())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "WINDOW section is not supported");
    if (query_tree.hasPrewhere())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "PREWHERE section is not supported");
    if (query_tree.hasLimitBy())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "LIMIT BY section is not supported");
    if (query_tree.hasOffset())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "OFFSET section is not supported");

    if (query_tree.hasLimit())
    {
        if (const auto & limit = query_tree.getLimit()->as<ConstantNode>())
            options->limit(limit->getValue().safeGet<UInt64>());
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unknown LIMIT AST");
    }

    if (query_tree.hasOrderBy())
    {
        document sort{};
        for (const auto & child : query_tree.getOrderByNode()->getChildren())
        {
            if (const auto & sort_node = child->as<SortNode>())
            {
                if (sort_node->withFill() || sort_node->hasFillTo() || sort_node->hasFillFrom() || sort_node->hasFillStep())
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only simple sort is supported");
                if (const auto & column = sort_node->getExpression()->as<ColumnNode>())
                    sort.append(kvp(column->getColumnName(), sort_node->getSortDirection() == SortDirection::ASCENDING ? 1 : -1));
                else
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only simple sort is supported");
            }
            else
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only simple sort is supported");
        }
        LOG_DEBUG(log, "MongoDB sort has built: '{}'", bsoncxx::to_json(sort));
        options->sort(sort.extract());
    }

    document projection{};
    for (const auto & column : sample_block)
        projection.append(kvp(column.name, 1));
    LOG_DEBUG(log, "MongoDB projection has built: '{}'", bsoncxx::to_json(projection));
    options->projection(projection.extract());

    if (query_tree.hasWhere())
    {
        auto filter = visitWhereFunction(query_tree.getWhere()->toAST()->as<ASTFunction>());
        LOG_DEBUG(log, "MongoDB query has built: '{}'", bsoncxx::to_json(filter));
        return filter;
    }

    return make_document();
}


void registerStorageMongoDB(StorageFactory & factory)
{
    factory.registerStorage("MongoDB", [](const StorageFactory::Arguments & args)
    {
        return std::make_shared<StorageMongoDB>(
            args.table_id,
            StorageMongoDB::getConfiguration(args.engine_args, args.getLocalContext()),
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
