#include "config.h"

#if USE_MONGODB
#include <memory>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/SortNode.h>
#include <Formats/BSONTypes.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTIdentifier.h>
#include <Processors/Sources/MongoDBSource.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMongoDB.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Common/parseAddress.h>
#include <Common/ErrorCodes.h>
#include <Common/BSONCXXHelper.h>
#include <Core/Settings.h>
#include <Core/Joins.h>

#include <bsoncxx/json.hpp>

using bsoncxx::builder::basic::document;
using bsoncxx::builder::basic::make_document;
using bsoncxx::builder::basic::make_array;
using bsoncxx::builder::basic::kvp;
using bsoncxx::to_json;

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int NOT_IMPLEMENTED;
}

namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsBool mongodb_throw_on_unsupported_query;
}

using BSONCXXHelper::fieldAsBSONValue;
using BSONCXXHelper::fieldAsOID;

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

    auto options = mongocxx::options::find{};

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
    if (func == "equals")
        return "$eq";
    if (func == "notEquals")
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

    return "";
}

template <typename OnError>
std::optional<bsoncxx::document::value> StorageMongoDB::visitWhereFunction(
    const ContextPtr & context,
    const FunctionNode * func,
    const JoinNode * join_node,
    OnError on_error)
{
    if (func->getArguments().getNodes().empty())
        return {};

    if (const auto & column = func->getArguments().getNodes().at(0)->as<ColumnNode>())
    {
        // Skip unknown columns, which don't belong to the table.
        const auto & table = column->getColumnSource()->as<TableNode>();
        if (!table)
            return {};

        // Skip columns from other tables in JOIN queries.
        if (table->getStorage()->getStorageID() != this->getStorageID())
            return {};
        if (join_node && column->getColumnSource() != join_node->getLeftTableExpression())
            return {};

        // Only these function can have exactly one argument and be passed to MongoDB.
        if (func->getFunctionName() == "isNull")
            return make_document(kvp(column->getColumnName(), make_document(kvp("$eq", bsoncxx::types::b_null{}))));
        if (func->getFunctionName() == "isNotNull")
            return make_document(kvp(column->getColumnName(), make_document(kvp("$ne", bsoncxx::types::b_null{}))));
        if (func->getFunctionName() == "empty")
            return make_document(kvp(column->getColumnName(), make_document(kvp("$in", make_array(bsoncxx::types::b_null{}, "")))));
        if (func->getFunctionName() == "notEmpty")
            return make_document(kvp(column->getColumnName(), make_document(kvp("$nin", make_array(bsoncxx::types::b_null{}, "")))));

        auto func_name = mongoFuncName(func->getFunctionName());
        if (func_name.empty())
        {
            on_error(func);
            return {};
        }

        if (func->getArguments().getNodes().size() == 2)
        {
            const auto & value = func->getArguments().getNodes().at(1);

            if (const auto & const_value = value->as<ConstantNode>())
            {
                std::optional<bsoncxx::types::bson_value::value> func_value{};
                if (column->getColumnName() == "_id")
                    func_value = fieldAsOID(const_value->getValue());
                else
                    func_value = fieldAsBSONValue(const_value->getValue(), const_value->getResultType());

                if (func_name == "$in" && func_value->view().type() != bsoncxx::v_noabi::type::k_array)
                    func_name = "$eq";
                if (func_name == "$nin" && func_value->view().type() != bsoncxx::v_noabi::type::k_array)
                    func_name = "$ne";

                return make_document(kvp(column->getColumnName(), make_document(kvp(func_name, std::move(*func_value)))));
            }

            if (const auto & func_value = value->as<FunctionNode>())
                if (const auto & res_value = visitWhereFunction(context, func_value, join_node, on_error); res_value.has_value())
                    return make_document(kvp(column->getColumnName(), make_document(kvp(func_name, *res_value))));
        }
    }
    else
    {
        auto arr = bsoncxx::builder::basic::array{};
        for (const auto & elem : func->getArguments().getNodes())
        {
            if (const auto & elem_func = elem->as<FunctionNode>())
                if (const auto & res_value = visitWhereFunction(context, elem_func, join_node, on_error); res_value.has_value())
                    arr.append(*res_value);
        }
        if (!arr.view().empty())
        {
            auto func_name = mongoFuncName(func->getFunctionName());
            if (func_name.empty())
            {
                on_error(func);
                return {};
            }
            return make_document(kvp(func_name, arr));
        }
    }

    on_error(func);
    return {};
}

bsoncxx::document::value StorageMongoDB::buildMongoDBQuery(const ContextPtr & context, mongocxx::options::find & options, const SelectQueryInfo & query, const Block & sample_block)
{
    document projection{};
    for (const auto & column : sample_block)
        projection.append(kvp(column.name, 1));
    LOG_DEBUG(log, "MongoDB projection has built: '{}'", bsoncxx::to_json(projection));
    options.projection(projection.extract());

    bool throw_on_error = context->getSettingsRef()[Setting::mongodb_throw_on_unsupported_query];

    if (!context->getSettingsRef()[Setting::allow_experimental_analyzer])
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MongoDB storage does not support 'allow_experimental_analyzer = 0' setting");
        return make_document();
    }

    const auto & query_tree = query.query_tree->as<QueryNode &>();

    if (throw_on_error)
    {
        if (query_tree.hasHaving())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "HAVING section is not supported. You can disable this error with 'SET mongodb_throw_on_unsupported_query=0', but this may cause poor performance, and is highly not recommended");
        if (query_tree.hasGroupBy())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "GROUP BY section is not supported. You can disable this error with 'SET mongodb_throw_on_unsupported_query=0', but this may cause poor performance, and is highly not recommended");
        if (query_tree.hasWindow())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "WINDOW section is not supported. You can disable this error with 'SET mongodb_throw_on_unsupported_query=0', but this may cause poor performance, and is highly not recommended");
        if (query_tree.hasPrewhere())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "PREWHERE section is not supported. You can disable this error with 'SET mongodb_throw_on_unsupported_query=0', but this may cause poor performance, and is highly not recommended");
        if (query_tree.hasLimitBy())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "LIMIT BY section is not supported. You can disable this error with 'SET mongodb_throw_on_unsupported_query=0', but this may cause poor performance, and is highly not recommended");
        if (query_tree.hasOffset())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "OFFSET section is not supported. You can disable this error with 'SET mongodb_throw_on_unsupported_query=0', but this may cause poor performance, and is highly not recommended");
    }

    auto on_error = [&] (const auto * node)
    {
        /// Reset limit, because if we omit ORDER BY, it should not be applied
        options.limit(0);

        if (throw_on_error)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Only simple queries are supported, failed to convert expression '{}' to MongoDB query. "
                "You can disable this restriction with 'SET mongodb_throw_on_unsupported_query=0', to read the full table and process on ClickHouse side (this may cause poor performance)", node->formatASTForErrorMessage());
        LOG_WARNING(log, "Failed to build MongoDB query for '{}'", node ? node->formatASTForErrorMessage() : "<unknown>");
    };


    if (query_tree.hasLimit())
    {
        if (const auto & limit = query_tree.getLimit()->as<ConstantNode>())
            options.limit(limit->getValue().safeGet<UInt64>());
        else
            on_error(query_tree.getLimit().get());
    }

    if (query_tree.hasOrderBy())
    {
        document sort{};
        for (const auto & child : query_tree.getOrderByNode()->getChildren())
        {
            if (const auto * sort_node = child->as<SortNode>())
            {
                if (sort_node->withFill() || sort_node->hasFillTo() || sort_node->hasFillFrom() || sort_node->hasFillStep())
                    on_error(sort_node);

                if (const auto & column = sort_node->getExpression()->as<ColumnNode>())
                    sort.append(kvp(column->getColumnName(), sort_node->getSortDirection() == SortDirection::ASCENDING ? 1 : -1));
                else
                    on_error(sort_node);
            }
            else
                on_error(sort_node);
        }
        if (!sort.view().empty())
        {
            LOG_DEBUG(log, "MongoDB sort has built: '{}'", bsoncxx::to_json(sort));
            options.sort(sort.extract());
        }
    }

    if (query_tree.hasWhere())
    {
        const auto & join_tree = query_tree.getJoinTree();
        const auto * join_node = join_tree->as<JoinNode>();
        bool allow_where = true;
        if (join_node)
        {
            if (join_node->getKind() == JoinKind::Left)
                allow_where = join_node->getLeftTableExpression()->isEqual(*query.table_expression);
            else if (join_node->getKind() == JoinKind::Right)
                allow_where = join_node->getRightTableExpression()->isEqual(*query.table_expression);
            else
                allow_where = (join_node->getKind() == JoinKind::Inner);
        }

        if (allow_where)
        {
            std::optional<bsoncxx::document::value> filter{};
            if (const auto & func = query_tree.getWhere()->as<FunctionNode>())
                filter = visitWhereFunction(context, func, join_node, on_error);

            else if (const auto & const_expr = query_tree.getWhere()->as<ConstantNode>())
            {
                if (const_expr->hasSourceExpression())
                {
                    if (const auto & func_expr = const_expr->getSourceExpression()->as<FunctionNode>())
                        filter = visitWhereFunction(context, func_expr, join_node, on_error);
                }
            }

            if (filter.has_value())
            {
                LOG_DEBUG(log, "MongoDB query has built: '{}'.", bsoncxx::to_json(*filter));
                return std::move(*filter);
            }
        }
        else
            on_error(join_node);
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
