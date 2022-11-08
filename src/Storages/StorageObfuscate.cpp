#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Storages/StorageObfuscate.h>
#include <Storages/StorageFactory.h>
#include <Storages/SelectQueryDescription.h>

#include <Common/typeid_cast.h>

#include <QueryPipeline/Pipe.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ObfuscateStep.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
}

StorageObfuscate::StorageObfuscate(
    const StorageID & table_id_,
    const ASTCreateQuery & query,
    const ColumnsDescription & columns_,
    const String & comment)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setComment(comment);

    if (!query.select)
        throw Exception("SELECT query is not specified for " + getName(), ErrorCodes::INCORRECT_QUERY);
    SelectQueryDescription description;

    description.inner_query = query.select->ptr();
    storage_metadata.setSelectQuery(description);
    setInMemoryMetadata(storage_metadata);
}

void StorageObfuscate::read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum /*processed_stage*/,
        const size_t /*max_block_size*/,
        const size_t /*num_streams*/)
{
    ASTPtr current_inner_query = storage_snapshot->metadata->getSelectQuery().inner_query;

    if (query_info.view_query)
    {
        if (!query_info.view_query->as<ASTSelectWithUnionQuery>())
            throw Exception("Unexpected optimized VIEW query", ErrorCodes::LOGICAL_ERROR);
        current_inner_query = query_info.view_query->clone();
    }

    auto options = SelectQueryOptions(QueryProcessingStage::Complete, 0, false, query_info.settings_limit_offset_done);
    InterpreterSelectWithUnionQuery interpreter(current_inner_query, context, options, column_names);
    interpreter.addStorageLimits(*query_info.storage_limits);
    interpreter.buildQueryPlan(query_plan);

    /// It's expected that the columns read from storage are not constant.
    /// Because method 'getSampleBlockForColumns' is used to obtain a structure of result in InterpreterSelectQuery.
    auto materializing_actions = std::make_shared<ActionsDAG>(query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName());
    materializing_actions->addMaterializingOutputActions();

    auto materializing = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), std::move(materializing_actions));
    materializing->setStepDescription("Materialize constants after VIEW subquery");
    query_plan.addStep(std::move(materializing));

    auto obfuscation = std::make_unique<ObfuscateStep>(query_plan.getCurrentDataStream(), context->getTempDataOnDisk());
    query_plan.addStep(std::move(obfuscation));
}

void registerStorageObfuscate(StorageFactory & factory)
{
    factory.registerStorage("Obfuscate", [](const StorageFactory::Arguments & args)
    {
        if (args.query.storage)
            throw Exception("Specifying ENGINE is not allowed for an Obfuscate", ErrorCodes::INCORRECT_QUERY);

        return std::make_shared<StorageObfuscate>(args.table_id, args.query, args.columns, args.comment);
    });
}

}
