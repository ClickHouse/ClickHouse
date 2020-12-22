#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/Context.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

#include <Storages/StorageView.h>
#include <Storages/StorageFactory.h>
#include <Storages/SelectQueryDescription.h>

#include <Common/typeid_cast.h>

#include <Processors/Pipe.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/QueryPlan/MaterializingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/SettingQuotaAndLimitsStep.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
}


StorageView::StorageView(
    const StorageID & table_id_,
    const ASTCreateQuery & query,
    const ColumnsDescription & columns_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);

    if (!query.select)
        throw Exception("SELECT query is not specified for " + getName(), ErrorCodes::INCORRECT_QUERY);

    SelectQueryDescription description;

    description.inner_query = query.select->ptr();
    storage_metadata.setSelectQuery(description);
    setInMemoryMetadata(storage_metadata);
}


Pipe StorageView::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum processed_stage,
    const size_t max_block_size,
    const unsigned num_streams)
{
    QueryPlan plan;
    read(plan, column_names, metadata_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
    return plan.convertToPipe();
}

void StorageView::read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum /*processed_stage*/,
        const size_t /*max_block_size*/,
        const unsigned /*num_streams*/)
{
    ASTPtr current_inner_query = metadata_snapshot->getSelectQuery().inner_query;

    if (query_info.view_query)
    {
        if (!query_info.view_query->as<ASTSelectWithUnionQuery>())
            throw Exception("Unexpected optimized VIEW query", ErrorCodes::LOGICAL_ERROR);
        current_inner_query = query_info.view_query->clone();
    }

    InterpreterSelectWithUnionQuery interpreter(current_inner_query, context, {}, column_names);
    interpreter.buildQueryPlan(query_plan);

    /// It's expected that the columns read from storage are not constant.
    /// Because method 'getSampleBlockForColumns' is used to obtain a structure of result in InterpreterSelectQuery.
    auto materializing = std::make_unique<MaterializingStep>(query_plan.getCurrentDataStream());
    materializing->setStepDescription("Materialize constants after VIEW subquery");
    query_plan.addStep(std::move(materializing));

    /// And also convert to expected structure.
    auto header = metadata_snapshot->getSampleBlockForColumns(column_names, getVirtuals(), getStorageID());
    auto convert_actions_dag = ActionsDAG::makeConvertingActions(
            query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName(),
            header.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name);

    auto converting = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), convert_actions_dag);
    converting->setStepDescription("Convert VIEW subquery result to VIEW table structure");
    query_plan.addStep(std::move(converting));
}

static ASTTableExpression * getFirstTableExpression(ASTSelectQuery & select_query)
{
    auto * select_element = select_query.tables()->children[0]->as<ASTTablesInSelectQueryElement>();

    if (!select_element->table_expression)
        throw Exception("Logical error: incorrect table expression", ErrorCodes::LOGICAL_ERROR);

    return select_element->table_expression->as<ASTTableExpression>();
}

void StorageView::replaceWithSubquery(ASTSelectQuery & outer_query, ASTPtr view_query, ASTPtr & view_name)
{
    ASTTableExpression * table_expression = getFirstTableExpression(outer_query);

    if (!table_expression->database_and_table_name)
    {
        // If it's a view table function, add a fake db.table name.
        if (table_expression->table_function && table_expression->table_function->as<ASTFunction>()->name == "view")
            table_expression->database_and_table_name = std::make_shared<ASTIdentifier>("__view");
        else
            throw Exception("Logical error: incorrect table expression", ErrorCodes::LOGICAL_ERROR);
    }

    DatabaseAndTableWithAlias db_table(table_expression->database_and_table_name);
    String alias = db_table.alias.empty() ? db_table.table : db_table.alias;

    view_name = table_expression->database_and_table_name;
    table_expression->database_and_table_name = {};
    table_expression->subquery = std::make_shared<ASTSubquery>();
    table_expression->subquery->children.push_back(view_query);
    table_expression->subquery->setAlias(alias);

    for (auto & child : table_expression->children)
        if (child.get() == view_name.get())
            child = view_query;
}

ASTPtr StorageView::restoreViewName(ASTSelectQuery & select_query, const ASTPtr & view_name)
{
    ASTTableExpression * table_expression = getFirstTableExpression(select_query);

    if (!table_expression->subquery)
        throw Exception("Logical error: incorrect table expression", ErrorCodes::LOGICAL_ERROR);

    ASTPtr subquery = table_expression->subquery;
    table_expression->subquery = {};
    table_expression->database_and_table_name = view_name;

    for (auto & child : table_expression->children)
        if (child.get() == subquery.get())
            child = view_name;
    return subquery->children[0];
}

void registerStorageView(StorageFactory & factory)
{
    factory.registerStorage("View", [](const StorageFactory::Arguments & args)
    {
        if (args.query.storage)
            throw Exception("Specifying ENGINE is not allowed for a View", ErrorCodes::INCORRECT_QUERY);

        return StorageView::create(args.table_id, args.query, args.columns);
    });
}

}
