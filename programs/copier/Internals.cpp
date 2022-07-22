#include "Internals.h"
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Transforms/SquashingChunksTransform.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/extractKeyExpressionList.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

ConfigurationPtr getConfigurationFromXMLString(const std::string & xml_data)
{
    std::stringstream ss(xml_data);         // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::XML::InputSource input_source{ss};
    return {new Poco::Util::XMLConfiguration{&input_source}};
}

String getQuotedTable(const String & database, const String & table)
{
    if (database.empty())
        return backQuoteIfNeed(table);

    return backQuoteIfNeed(database) + "." + backQuoteIfNeed(table);
}

String getQuotedTable(const DatabaseAndTableName & db_and_table)
{
    return getQuotedTable(db_and_table.first, db_and_table.second);
}


// Creates AST representing 'ENGINE = Distributed(cluster, db, table, [sharding_key])
std::shared_ptr<ASTStorage> createASTStorageDistributed(
        const String & cluster_name, const String & database, const String & table,
        const ASTPtr & sharding_key_ast)
{
    auto args = std::make_shared<ASTExpressionList>();
    args->children.emplace_back(std::make_shared<ASTLiteral>(cluster_name));
    args->children.emplace_back(std::make_shared<ASTIdentifier>(database));
    args->children.emplace_back(std::make_shared<ASTIdentifier>(table));
    if (sharding_key_ast)
        args->children.emplace_back(sharding_key_ast);

    auto engine = std::make_shared<ASTFunction>();
    engine->name = "Distributed";
    engine->arguments = args;

    auto storage = std::make_shared<ASTStorage>();
    storage->set(storage->engine, engine);

    return storage;
}


Block getBlockWithAllStreamData(QueryPipelineBuilder builder)
{
    builder.addTransform(std::make_shared<SquashingChunksTransform>(
        builder.getHeader(),
        std::numeric_limits<size_t>::max(),
        std::numeric_limits<size_t>::max()));

    auto cur_pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));
    Block block;
    PullingPipelineExecutor executor(cur_pipeline);
    executor.pull(block);

    return block;
}

bool isExtendedDefinitionStorage(const ASTPtr & storage_ast)
{
    const auto & storage = storage_ast->as<ASTStorage &>();
    return storage.partition_by || storage.order_by || storage.sample_by;
}

ASTPtr extractPartitionKey(const ASTPtr & storage_ast)
{
    String storage_str = queryToString(storage_ast);

    const auto & storage = storage_ast->as<ASTStorage &>();
    const auto & engine = storage.engine->as<ASTFunction &>();

    if (!endsWith(engine.name, "MergeTree"))
    {
        throw Exception(
                "Unsupported engine was specified in " + storage_str + ", only *MergeTree engines are supported",
                ErrorCodes::BAD_ARGUMENTS);
    }

    if (isExtendedDefinitionStorage(storage_ast))
    {
        if (storage.partition_by)
            return storage.partition_by->clone();

        static const char * all = "all";
        return std::make_shared<ASTLiteral>(Field(all, strlen(all)));
    }
    else
    {
        bool is_replicated = startsWith(engine.name, "Replicated");
        size_t min_args = is_replicated ? 3 : 1;

        if (!engine.arguments)
            throw Exception("Expected arguments in " + storage_str, ErrorCodes::BAD_ARGUMENTS);

        ASTPtr arguments_ast = engine.arguments->clone();
        ASTs & arguments = arguments_ast->children;

        if (arguments.size() < min_args)
            throw Exception("Expected at least " + toString(min_args) + " arguments in " + storage_str,
                            ErrorCodes::BAD_ARGUMENTS);

        ASTPtr & month_arg = is_replicated ? arguments[2] : arguments[1];
        return makeASTFunction("toYYYYMM", month_arg->clone());
    }
}

ASTPtr extractPrimaryKey(const ASTPtr & storage_ast)
{
    String storage_str = queryToString(storage_ast);

    const auto & storage = storage_ast->as<ASTStorage &>();
    const auto & engine = storage.engine->as<ASTFunction &>();

    if (!endsWith(engine.name, "MergeTree"))
    {
        throw Exception("Unsupported engine was specified in " + storage_str + ", only *MergeTree engines are supported",
                        ErrorCodes::BAD_ARGUMENTS);
    }

    if (!isExtendedDefinitionStorage(storage_ast))
    {
        throw Exception("Is not extended deginition storage " + storage_str + " Will be fixed later.",
                        ErrorCodes::BAD_ARGUMENTS);
    }

    if (storage.primary_key)
        return storage.primary_key->clone();

    return nullptr;
}


ASTPtr extractOrderBy(const ASTPtr & storage_ast)
{
    String storage_str = queryToString(storage_ast);

    const auto & storage = storage_ast->as<ASTStorage &>();
    const auto & engine = storage.engine->as<ASTFunction &>();

    if (!endsWith(engine.name, "MergeTree"))
    {
        throw Exception("Unsupported engine was specified in " + storage_str + ", only *MergeTree engines are supported",
                        ErrorCodes::BAD_ARGUMENTS);
    }

    if (!isExtendedDefinitionStorage(storage_ast))
    {
        throw Exception("Is not extended deginition storage " + storage_str + " Will be fixed later.",
                        ErrorCodes::BAD_ARGUMENTS);
    }

    if (storage.order_by)
        return storage.order_by->clone();

    throw Exception("ORDER BY cannot be empty", ErrorCodes::BAD_ARGUMENTS);
}

/// Wraps only identifiers with backticks.
std::string wrapIdentifiersWithBackticks(const ASTPtr & root)
{
    if (auto identifier = std::dynamic_pointer_cast<ASTIdentifier>(root))
        return backQuote(identifier->name());

    if (auto function = std::dynamic_pointer_cast<ASTFunction>(root))
        return function->name + '(' + wrapIdentifiersWithBackticks(function->arguments) + ')';

    if (auto expression_list = std::dynamic_pointer_cast<ASTExpressionList>(root))
    {
        Names function_arguments(expression_list->children.size());
        for (size_t i = 0; i < expression_list->children.size(); ++i)
            function_arguments[i] = wrapIdentifiersWithBackticks(expression_list->children[0]);
        return boost::algorithm::join(function_arguments, ", ");
    }

    throw Exception("Primary key could be represented only as columns or functions from columns.", ErrorCodes::BAD_ARGUMENTS);
}


Names extractPrimaryKeyColumnNames(const ASTPtr & storage_ast)
{
    const auto sorting_key_ast = extractOrderBy(storage_ast);
    const auto primary_key_ast = extractPrimaryKey(storage_ast);

    const auto sorting_key_expr_list = extractKeyExpressionList(sorting_key_ast);
    const auto primary_key_expr_list = primary_key_ast
                           ? extractKeyExpressionList(primary_key_ast) : sorting_key_expr_list->clone();

    /// Maybe we have to handle VersionedCollapsing engine separately. But in our case in looks pointless.

    size_t primary_key_size = primary_key_expr_list->children.size();
    size_t sorting_key_size = sorting_key_expr_list->children.size();

    if (primary_key_size > sorting_key_size)
        throw Exception("Primary key must be a prefix of the sorting key, but its length: "
                        + toString(primary_key_size) + " is greater than the sorting key length: " + toString(sorting_key_size),
                        ErrorCodes::BAD_ARGUMENTS);

    Names primary_key_columns;
    NameSet primary_key_columns_set;

    for (size_t i = 0; i < sorting_key_size; ++i)
    {
        /// Column name could be represented as a f_1(f_2(...f_n(column_name))).
        /// Each f_i could take one or more parameters.
        /// We will wrap identifiers with backticks to allow non-standart identifier names.
        String sorting_key_column = sorting_key_expr_list->children[i]->getColumnName();

        if (i < primary_key_size)
        {
            String pk_column = primary_key_expr_list->children[i]->getColumnName();
            if (pk_column != sorting_key_column)
                throw Exception("Primary key must be a prefix of the sorting key, but the column in the position "
                    + toString(i) + " is " + sorting_key_column +", not " + pk_column,
                ErrorCodes::BAD_ARGUMENTS);

            if (!primary_key_columns_set.emplace(pk_column).second)
                throw Exception("Primary key contains duplicate columns", ErrorCodes::BAD_ARGUMENTS);

            primary_key_columns.push_back(wrapIdentifiersWithBackticks(primary_key_expr_list->children[i]));
        }
    }

    return primary_key_columns;
}

bool isReplicatedTableEngine(const ASTPtr & storage_ast)
{
    const auto & storage = storage_ast->as<ASTStorage &>();
    const auto & engine = storage.engine->as<ASTFunction &>();

    if (!endsWith(engine.name, "MergeTree"))
    {
        String storage_str = queryToString(storage_ast);
        throw Exception(
                "Unsupported engine was specified in " + storage_str + ", only *MergeTree engines are supported",
                ErrorCodes::BAD_ARGUMENTS);
    }

    return startsWith(engine.name, "Replicated");
}

ShardPriority getReplicasPriority(const Cluster::Addresses & replicas, const std::string & local_hostname, UInt8 random)
{
    ShardPriority res;

    if (replicas.empty())
        return res;

    res.is_remote = 1;
    for (const auto & replica : replicas)
    {
        if (isLocalAddress(DNSResolver::instance().resolveHost(replica.host_name)))
        {
            res.is_remote = 0;
            break;
        }
    }

    res.hostname_difference = std::numeric_limits<size_t>::max();
    for (const auto & replica : replicas)
    {
        size_t difference = getHostNameDifference(local_hostname, replica.host_name);
        res.hostname_difference = std::min(difference, res.hostname_difference);
    }

    res.random = random;
    return res;
}

}
