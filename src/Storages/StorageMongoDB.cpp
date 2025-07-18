#include "config.h"

#if USE_MONGODB

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/SortNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Common/BSONCXXHelper.h>
#include <Common/ErrorCodes.h>
#include <Common/logger_useful.h>
#include <Common/parseAddress.h>
#include <Common/FieldVisitorToString.h>
#include <Core/Joins.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <Formats/BSONTypes.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTIdentifier.h>
#include <Processors/Sources/MongoDBSource.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMongoDB.h>
#include <Storages/checkAndGetLiteralArgument.h>

#include <bsoncxx/json.hpp>
#include <mongocxx/exception/logic_error.hpp>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

#include <memory>

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

void MongoDBConfiguration::checkHosts(const ContextPtr & context) const
{
    // Because domain records will be resolved inside the driver, we can't check resolved IPs for our restrictions.
    for (const auto & host : uri->hosts())
        context->getRemoteHostFilter().checkHostAndPort(host.name, toString(host.port));
}

StorageMongoDB::StorageMongoDB(
    const StorageID & table_id_,
    MongoDBConfiguration configuration_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment)
    : IStorage{table_id_}
    , configuration{std::move(configuration_)}
    , log(getLogger("StorageMongoDB (" + table_id_.getFullTableName() + ")"))
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

    bsoncxx::document::view_or_value mongo_query = buildMongoDBQuery(context, options, query_info, sample_block);
    return Pipe(std::make_shared<MongoDBSource>(*configuration.uri, configuration.collection, mongo_query,
        std::move(options), std::make_shared<const Block>(std::move(sample_block)), max_block_size));
}

static MongoDBConfiguration getConfigurationImpl(const StorageID * table_id, ASTs engine_args, ContextPtr context, bool allow_excessive_path_in_host)
{
    MongoDBConfiguration configuration;
    if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, context))
    {
        if (named_collection->has("uri"))
        {
            validateNamedCollection(*named_collection, {"uri", "collection"}, {"oid_columns"});
            configuration.uri = std::make_unique<mongocxx::uri>(named_collection->get<String>("uri"));
        }
        else
        {
            validateNamedCollection(*named_collection, {
                "host", "port", "user", "password", "database", "collection"}, {"options", "oid_columns"});
            String user = named_collection->get<String>("user");
            String auth_string;
            String escaped_password;
            Poco::URI::encode(named_collection->get<String>("password"), "!?#/'\",;:$&()[]*+=@", escaped_password);
            if (!user.empty())
                auth_string = fmt::format("{}:{}@", user, escaped_password);
            configuration.uri = std::make_unique<mongocxx::uri>(fmt::format("mongodb://{}{}:{}/{}?{}",
                                                          auth_string,
                                                          named_collection->get<String>("host"),
                                                          named_collection->get<String>("port"),
                                                          named_collection->get<String>("database"),
                                                          named_collection->getOrDefault<String>("options", "")));
        }
        configuration.collection = named_collection->get<String>("collection");
        if (named_collection->has("oid_columns"))
            boost::split(configuration.oid_fields, named_collection->get<String>("oid_columns"), boost::is_any_of(","));
    }
    else
    {
        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, context);

        if (engine_args.size() >= 5 && engine_args.size() <= 7)
        {
            configuration.collection = checkAndGetLiteralArgument<String>(engine_args[2], "collection");

            String options;
            if (engine_args.size() >= 6)
                options = checkAndGetLiteralArgument<String>(engine_args[5], "options");

            String user = checkAndGetLiteralArgument<String>(engine_args[3], "user");
            String auth_string;
            String escaped_password;
            Poco::URI::encode(checkAndGetLiteralArgument<String>(engine_args[4], "password"), "!?#/'\",;:$&()[]*+=@", escaped_password);
            if (!user.empty())
                auth_string = fmt::format("{}:{}@", user, escaped_password);

            auto host_port = checkAndGetLiteralArgument<String>(engine_args[0], "host:port");
            auto database_name = checkAndGetLiteralArgument<String>(engine_args[1], "database");
            auto parsed_host_port = parseAddress(host_port, 27017);
            try
            {
                configuration.uri = std::make_unique<mongocxx::uri>(
                    fmt::format("mongodb://{}{}:{}/{}?{}", auth_string, parsed_host_port.first, parsed_host_port.second, database_name, options));
            }
            catch (const mongocxx::logic_error & e)
            {
                auto pos = host_port.find('/');
                if (!allow_excessive_path_in_host || pos == String::npos)
                    throw;

                LOG_WARNING(getLogger("StorageMongoDB"), "Failed to parse MongoDB connection string: '{}', trying to remove everything after slash from the hostname", e.what());

                host_port = host_port.substr(0, pos);
                parsed_host_port = parseAddress(host_port, 27017);

                configuration.uri = std::make_unique<mongocxx::uri>(
                    fmt::format("mongodb://{}{}:{}/{}?{}", auth_string, parsed_host_port.first, parsed_host_port.second, database_name, options));

                context->addOrUpdateWarningMessage(
                    Context::WarningType::OBSOLETE_MONGO_TABLE_DEFINITION,
                    PreformattedMessage::create(
                        "The first argument in '{}' table definition with MongoDB engine contains a path which was ignored. "
                        "To fix this, either use a complete MongoDB connection string with schema and database name as the first argument, "
                        "or use only host:port format and specify database name and other parameters separately in the table engine definition. "
                        "In future versions, this will be an error when loading the table.",
                        table_id ? table_id->getNameForLogs() : ""));
            }

            if (engine_args.size() == 7)
                boost::split(configuration.oid_fields,
                    checkAndGetLiteralArgument<String>(engine_args[6], "oid_columns"), boost::is_any_of(","));
        }
        else if (engine_args.size() == 2 || engine_args.size() == 3)
        {
            configuration.collection = checkAndGetLiteralArgument<String>(engine_args[1], "database");
            configuration.uri =  std::make_unique<mongocxx::uri>(checkAndGetLiteralArgument<String>(engine_args[0], "host"));
            if (engine_args.size() == 3)
                boost::split(configuration.oid_fields,
                    checkAndGetLiteralArgument<String>(engine_args[2], "oid_columns"), boost::is_any_of(","));
        }
        else
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                                "Incorrect number of arguments. Example usage: "
                                "MongoDB('host:port', 'database', 'collection', 'user', 'password'[, options[, oid_columns]]) or MongoDB('uri', 'collection'[, oid columns]).");
    }

    configuration.checkHosts(context);

    return configuration;
}

MongoDBConfiguration StorageMongoDB::getConfiguration(ASTs engine_args, ContextPtr context)
{
    return getConfigurationImpl(nullptr, std::move(engine_args), context, false);
}

static std::string mongoFuncName(const std::string & func)
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

static std::string invertMongoComparison(const std::string & func)
{
    if (func == "$gt")
        return "$lt";
    if (func == "$gte")
        return "$lte";
    if (func == "$lt")
        return "$gt";
    if (func == "$lte")
        return "$gte";

    return func;
}

static const ColumnNode * getColumnNode(const QueryTreeNodePtr & node, const JoinNode * join_node, const StorageID & storage_id)
{
    const auto * column = node->as<ColumnNode>();
    if (!column)
        return {};

    // Skip unknown columns, which don't belong to the table.
    const auto & table = column->getColumnSource()->as<TableNode>();
    const auto & table_function = column->getColumnSource()->as<TableFunctionNode>();
    if (!table && !table_function)
        return {};

    // Skip columns from other tables in JOIN queries.
    if (table && table->getStorage()->getStorageID() != storage_id)
        return {};
    if (table_function && table_function->getStorage()->getStorageID() != storage_id)
        return {};
    if (join_node && column->getColumnSource() != join_node->getLeftTableExpression())
        return {};

    return column;
}

std::optional<bsoncxx::document::value> StorageMongoDB::visitWhereConstant(
    const ContextPtr & context,
    const ConstantNode * const_node,
    const JoinNode * join_node)
{
    if (const_node->hasSourceExpression())
    {
        if (const auto * func_node = const_node->getSourceExpression()->as<FunctionNode>())
            return visitWhereFunction(context, func_node, join_node);
    }
    return {};
}

std::optional<bsoncxx::document::value> StorageMongoDB::visitWhereFunctionArguments(
    const ColumnNode * column_node,
    const ConstantNode * const_node,
    const FunctionNode * func,
    bool invert_comparison)
{
    auto func_name = mongoFuncName(func->getFunctionName());

    if (func_name.empty())
    {
        LOG_DEBUG(log, "MongoDB function name not found for '{}'", func->getFunctionName());
        return {};
    }

    if (invert_comparison)
    {
        func_name = invertMongoComparison(func_name);
    }

    auto const_type = const_node->getResultType();
    auto column_type = column_node->getResultType();
    auto const_value = const_node->getValue();

    bool is_const_number = WhichDataType(const_type).isNumber();
    bool is_column_number = WhichDataType(column_type).isNumber();

    if (func_name == "$in" || func_name == "$nin")
    {
        if (const_value.getType() == Field::Types::Array)
        {
            column_type = std::make_shared<DataTypeArray>(column_type);
        }
        else if (const_value.getType() == Field::Types::Tuple)
        {
            auto & value_tuple = const_value.safeGet<Tuple>();
            const_value = Array(value_tuple.begin(), value_tuple.end());
            column_type = std::make_shared<DataTypeArray>(column_type);
        }
    }

    /// Conversion is required because MongoDB cannot perform implicit cast and the result of WHERE clause may be incorrect.
    /// But implicit conversion between numbers works well and doesn't affect the result of WHERE clause.
    if (!const_type->equals(*column_type) && (!is_const_number || !is_column_number))
    {
        auto converted_value = convertFieldToType(const_value, *column_type, const_type.get());

        if (converted_value.isNull())
        {
            auto value_string = applyVisitor(FieldVisitorToString(), const_value);
            LOG_DEBUG(log, "Cannot convert constant value {} to column type {}", value_string, column_type->getName());
            return {};
        }

        const_type = column_type;
        const_value = std::move(converted_value);
    }

    auto func_value = BSONCXXHelper::fieldAsBSONValue(
        const_value,
        const_type,
        configuration.isOidColumn(column_node->getColumnName()));

    if (func_name == "$in" && func_value.view().type() != bsoncxx::v_noabi::type::k_array)
        func_name = "$eq";
    if (func_name == "$nin" && func_value.view().type() != bsoncxx::v_noabi::type::k_array)
        func_name = "$ne";

    return make_document(kvp(column_node->getColumnName(), make_document(kvp(func_name, std::move(func_value)))));
}

std::optional<bsoncxx::document::value> StorageMongoDB::visitWhereFunction(
    const ContextPtr & context,
    const FunctionNode * func,
    const JoinNode * join_node)
{
    const auto & argumnet_nodes = func->getArguments().getNodes();
    if (func->getArguments().getNodes().empty())
        return {};

    if (argumnet_nodes.size() == 1)
    {
        const auto * column = getColumnNode(func->getArguments().getNodes().at(0), join_node, this->getStorageID());
        if (!column)
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

        return {};
    }

    if (argumnet_nodes.size() == 2)
    {
        const auto * left_column = getColumnNode(func->getArguments().getNodes().at(0), join_node, getStorageID());
        const auto * right_const = func->getArguments().getNodes().at(1)->as<ConstantNode>();

        if (left_column && right_const)
            return visitWhereFunctionArguments(left_column, right_const, func, false);

        /// We cannot invert "in" and "notIn" functions.
        if (func->getFunctionName() == "in" || func->getFunctionName() == "notIn")
            return {};

        const auto * left_const = func->getArguments().getNodes().at(0)->as<ConstantNode>();
        const auto * right_column = getColumnNode(func->getArguments().getNodes().at(1), join_node, getStorageID());

        if (left_const && right_column)
            return visitWhereFunctionArguments(right_column, left_const, func, true);
    }

    auto func_name = mongoFuncName(func->getFunctionName());
    if (func_name.empty())
        return {};

    auto arr = bsoncxx::builder::basic::array{};

    for (const auto & elem : func->getArguments().getNodes())
    {
        auto res_value = visitWhereNode(context, elem, join_node);
        if (!res_value)
            return {};

        arr.append(*res_value);
    }

    return make_document(kvp(func_name, arr));
}

std::optional<bsoncxx::document::value> StorageMongoDB::visitWhereNode(
    const ContextPtr & context,
    const QueryTreeNodePtr & where_node,
    const JoinNode * join_node)
{
    if (const auto * func = where_node->as<FunctionNode>())
        return visitWhereFunction(context, func, join_node);

    if (const auto * const_expr = where_node->as<ConstantNode>())
        return visitWhereConstant(context, const_expr, join_node);

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
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MongoDB storage does not support 'enable_analyzer = 0' setting");
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

    const ConstantNode * limit = nullptr;
    const ConstantNode * offset = nullptr;

    if (query_tree.hasLimit())
    {
        limit = query_tree.getLimit()->as<ConstantNode>();
        if (!limit)
        {
            on_error(query_tree.getLimit().get());
        }
    }

    if (query_tree.hasOffset())
    {
        offset = query_tree.getOffset()->as<ConstantNode>();
        if (!offset)
        {
            on_error(query_tree.getOffset().get());
            limit = nullptr;
        }
    }

    if (limit || offset)
    {
        /// Otherwise it was already checked above.
        if (!query_tree.hasGroupBy() && !query_tree.hasLimitBy())
        {
            auto limit_value = limit ? limit->getValue().safeGet<UInt64>() : 0;
            auto offset_value = offset ? offset->getValue().safeGet<UInt64>() : 0;

            options.limit(limit_value + offset_value);
        }
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

        if (!allow_where)
        {
            on_error(join_node);
            return make_document();
        }

        const auto & where_node = query_tree.getWhere();
        auto filter = visitWhereNode(context, query_tree.getWhere(), join_node);

        if (!filter)
        {
            on_error(where_node.get());
            return make_document();
        }

        LOG_DEBUG(log, "MongoDB query has built: '{}'.", bsoncxx::to_json(*filter));
        return std::move(*filter);
    }

    return make_document();
}


void registerStorageMongoDB(StorageFactory & factory)
{
    factory.registerStorage("MongoDB", [](const StorageFactory::Arguments & args)
    {
        /// Allow loading tables with excessive path in host parameter created on older ClickHouse versions
        /// (that used the previous Poco-based MongoDB implementation which allowed it).
        /// TODO: we can remove it after ClickHouse 27.5, it should be enough time for users to migrate.
        bool allow_excessive_path_in_host = args.mode > LoadingStrictnessLevel::CREATE;
        return std::make_shared<StorageMongoDB>(
            args.table_id,
            getConfigurationImpl(&args.table_id, args.engine_args, args.getLocalContext(), allow_excessive_path_in_host),
            args.columns,
            args.constraints,
            args.comment);
    },
    {
        .source_access_type = AccessTypeObjects::Source::MONGO,
    });
}

}
#endif
