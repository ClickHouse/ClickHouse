#include "config.h"

#if USE_MONGODB
#include <memory>

#include <Analyzer/QueryNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/SortNode.h>
#include <Formats/BSONTypes.h>
#include <DataTypes/FieldToDataType.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
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
    ContextPtr context,
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

    return Pipe(std::make_shared<MongoDBSource>(*configuration.uri, configuration.collection, buildMongoDBQuery(context, options, query_info, sample_block),
        std::move(options), sample_block, max_block_size));
}

MongoDBConfiguration StorageMongoDB::getConfiguration(ASTs engine_args, ContextPtr context)
{
    MongoDBConfiguration configuration;
    if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, context))
    {
        if (named_collection->has("uri"))
        {
            validateNamedCollection(*named_collection, {"collection"}, {"uri"});
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

std::string mongoFuncName(const std::string & func)
{
    if (func == "equals" || func == "isNull")
        return "$eq";
    if (func == "notEquals" || func == "isNotNull")
        return "$ne";
    if (func == "greaterThan" || func == "greater")
        return "$gt";
    if (func == "lessThan" || func == "less")
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

bsoncxx::types::bson_value::value fieldAsBSONValue(const Field & field, const DataTypePtr & type)
{
    switch (type->getTypeId())
    {
        case TypeIndex::String:
            return field.get<String>();
        case TypeIndex::UInt8:
        {
            if (isBool(type))
                return field.get<UInt8>() != 0;
            return static_cast<Int32>(field.get<UInt8>());
        }
        case TypeIndex::UInt16:
            return static_cast<Int32>(field.get<UInt16>());
        case TypeIndex::UInt32:
            return static_cast<Int32>(field.get<UInt32>());
        case TypeIndex::UInt64:
            return static_cast<Int64>(field.get<UInt64>());
        case TypeIndex::Int8:
            return field.get<Int8 &>();
        case TypeIndex::Int16:
            return field.get<Int16>();
        case TypeIndex::Int32:
            return field.get<Int32>();
        case TypeIndex::Int64:
            return field.get<Int64>();
        case TypeIndex::Float32:
            return field.get<Float32>();
        case TypeIndex::Float64:
            return field.get<Float64>();
        case TypeIndex::Date:
            return std::chrono::milliseconds(field.get<UInt16>() * 1000);
        case TypeIndex::Date32:
            return std::chrono::milliseconds(field.get<Int32>() * 1000);
        case TypeIndex::DateTime:
            return std::chrono::milliseconds(field.get<UInt32>() * 1000);
        case TypeIndex::DateTime64:
            return std::chrono::milliseconds(field.get<Decimal64>().getValue());
        case TypeIndex::UUID:
            return static_cast<String>(formatUUID(field.get<UUID>()));
        case TypeIndex::Tuple:
        {
            auto arr = array();
            for (const auto & elem : field.get<Tuple &>())
                arr.append(fieldAsBSONValue(elem, applyVisitor(FieldToDataType(), elem)));
            return arr.view();
        }
        case TypeIndex::Array:
        {
            auto arr = array();
            for (const auto & elem : field.get<Array &>())
                arr.append(fieldAsBSONValue(elem, applyVisitor(FieldToDataType(), elem)));
            return arr.view();
        }
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Fields with type '{}' is not supported", type->getPrettyName());
    }
}

void checkLiteral(const Field & field, std::string * func)
{
    if (*func == "$in" && field.getType() != Field::Types::Array && field.getType() != Field::Types::Tuple)
        *func = "$eq";

    if (*func == "$nin" && field.getType() != Field::Types::Array && field.getType() != Field::Types::Tuple)
        *func = "$ne";
}

bsoncxx::document::value StorageMongoDB::visitWhereFunction(ContextPtr context, const ASTFunction * func)
{
    auto func_name = mongoFuncName(func->name);

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

            if (auto result = tryEvaluateConstantExpression(explist->children.at(1), context))
            {
                checkLiteral(result->first, &func_name);
                if (identifier->shortName() == "_id")
                {
                    switch (result->second->getColumnType())
                    {
                        case TypeIndex::String:
                            return make_document(kvp(identifier->shortName(), make_document(kvp(func_name, bsoncxx::oid(result->first.get<String>())))));
                        case TypeIndex::Tuple:
                        {
                            auto oid_arr = array();
                            for (const auto & elem : result->first.get<Tuple &>())
                            {
                                if (elem.getType() != Field::Types::String)
                                    throw Exception(ErrorCodes::TYPE_MISMATCH, "{} can't be converted to oid", elem.getTypeName());
                                oid_arr.append(bsoncxx::oid(elem.get<String>()));
                            }
                            return make_document(kvp(identifier->shortName(), make_document(kvp(func_name, oid_arr))));
                        }
                        case TypeIndex::Array:
                        {
                            auto oid_arr = array();
                            for (const auto & elem : result->first.get<Array &>())
                            {
                                if (elem.getType() != Field::Types::String)
                                    throw Exception(ErrorCodes::TYPE_MISMATCH, "{} can't be converted to oid", elem.getTypeName());
                                oid_arr.append(bsoncxx::oid(elem.get<String>()));
                            }
                            return make_document(kvp(identifier->shortName(), make_document(kvp(func_name, oid_arr))));
                        }
                        default:
                            throw Exception(ErrorCodes::TYPE_MISMATCH, "{} can't be converted to oid", result->second->getPrettyName());
                    }
                }

                return make_document(kvp(identifier->shortName(), make_document(kvp(func_name, fieldAsBSONValue(result->first, result->second)))));
            }

            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only constant expressions are supported in WHERE section");
        }

        auto arr = array();
        for (const auto & child : explist->children)
        {
            if (const auto & child_func = child->as<ASTFunction>())
                arr.append(visitWhereFunction(context, child_func));
            else
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only constant expressions are supported in WHERE section");
        }

        return make_document(kvp(func_name, std::move(arr)));
    }

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only constant expressions are supported in WHERE section");
}

bsoncxx::document::value StorageMongoDB::buildMongoDBQuery(ContextPtr context, mongocxx::options::find & options, const SelectQueryInfo & query, const Block & sample_block)
{
    auto & query_tree = query.query_tree->as<QueryNode &>();

    if (context->getSettingsRef().mongodb_fail_on_query_build_error)
    {
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
    }

    if (query_tree.hasLimit())
    {
        try
        {
            if (const auto & limit = query_tree.getLimit()->as<ConstantNode>())
                options.limit(limit->getValue().safeGet<UInt64>());
            else
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unknown LIMIT AST");
        }
        catch (...)
        {
            if (context->getSettingsRef().mongodb_fail_on_query_build_error)
                throw;
            tryLogCurrentException(log);
        }
    }

    if (query_tree.hasOrderBy())
    {
        try
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
            options.sort(sort.extract());
        }
        catch (...)
        {
            if (context->getSettingsRef().mongodb_fail_on_query_build_error)
                throw;
            tryLogCurrentException(log);
        }
    }

    document projection{};
    for (const auto & column : sample_block)
        projection.append(kvp(column.name, 1));
    LOG_DEBUG(log, "MongoDB projection has built: '{}'", bsoncxx::to_json(projection));
    options.projection(projection.extract());

    if (query_tree.hasWhere())
    {
        try
        {
            auto ast = query_tree.getWhere()->toAST();
            if (const auto & func = ast->as<ASTFunction>())
            {
                auto filter = visitWhereFunction(context, func);
                LOG_DEBUG(log, "MongoDB query has built: '{}'", bsoncxx::to_json(filter));
                return filter;
            }
        }
        catch (...)
        {
            if (context->getSettingsRef().mongodb_fail_on_query_build_error)
                throw;
            tryLogCurrentException(log);
        }
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
