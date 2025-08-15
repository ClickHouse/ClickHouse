#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/NormalizeSelectWithUnionQueryVisitor.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Storages/AlterCommands.h>
#include <Storages/StorageView.h>
#include <Storages/StorageFactory.h>
#include <Storages/SelectQueryDescription.h>

#include <Common/typeid_cast.h>

#include <Core/Settings.h>

#include <QueryPipeline/Pipe.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>

#include <Interpreters/ReplaceQueryParameterVisitor.h>
#include <Parsers/QueryParameterVisitor.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsBool extremes;
    extern const SettingsUInt64 max_result_rows;
    extern const SettingsUInt64 max_result_bytes;
}

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}


namespace
{

bool isNullableOrLcNullable(DataTypePtr type)
{
    if (type->isNullable())
        return true;

    if (const auto * lc_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
        return lc_type->getDictionaryType()->isNullable();

    return false;
}

/// Returns `true` if there are nullable column in src but corresponding column in dst is not
bool changedNullabilityOneWay(const Block & src_block, const Block & dst_block)
{
    std::unordered_map<String, bool> src_nullable;
    for (const auto & col : src_block)
        src_nullable[col.name] = isNullableOrLcNullable(col.type);

    for (const auto & col : dst_block)
    {
        if (!isNullableOrLcNullable(col.type) && src_nullable[col.name])
            return true;
    }
    return false;
}

bool hasJoin(const ASTSelectQuery & select)
{
    const auto & tables = select.tables();
    if (!tables || tables->children.size() < 2)
        return false;

    const auto & joined_table = tables->children[1]->as<ASTTablesInSelectQueryElement &>();
    return joined_table.table_join != nullptr;
}

bool hasJoin(const ASTSelectWithUnionQuery & ast)
{
    for (const auto & child : ast.list_of_selects->children)
    {
        if (const auto * select = child->as<ASTSelectQuery>(); select && hasJoin(*select))
            return true;
    }
    return false;
}

/** There are no limits on the maximum size of the result for the view.
  *  Since the result of the view is not the result of the entire query.
  */
ContextPtr getViewContext(ContextPtr context, const StorageSnapshotPtr & storage_snapshot)
{
    auto view_context = storage_snapshot->metadata->getSQLSecurityOverriddenContext(context);
    Settings view_settings = view_context->getSettingsCopy();
    view_settings[Setting::max_result_rows] = 0;
    view_settings[Setting::max_result_bytes] = 0;
    view_settings[Setting::extremes] = false;
    view_context->setSettings(view_settings);
    return view_context;
}

}

StorageView::StorageView(
    const StorageID & table_id_,
    const ASTCreateQuery & query,
    const ColumnsDescription & columns_,
    const String & comment,
    bool is_parameterized_view_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    if (!is_parameterized_view_)
    {
        /// If CREATE query is to create parameterized view, then we dont want to set columns
        if (!query.isParameterizedView())
            storage_metadata.setColumns(columns_);
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setComment(comment);
    if (query.sql_security)
        storage_metadata.setSQLSecurity(query.sql_security->as<ASTSQLSecurity &>());

    if (!query.select)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "SELECT query is not specified for {}", getName());
    SelectQueryDescription description;

    description.inner_query = query.select->ptr();

    NormalizeSelectWithUnionQueryVisitor::Data data{SetOperationMode::Unspecified};
    NormalizeSelectWithUnionQueryVisitor{data}.visit(description.inner_query);

    is_parameterized_view = is_parameterized_view_ || query.isParameterizedView();
    storage_metadata.setSelectQuery(description);
    setInMemoryMetadata(storage_metadata);
}

void StorageView::read(
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
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected optimized VIEW query");
        current_inner_query = query_info.view_query->clone();
    }

    auto options = SelectQueryOptions(QueryProcessingStage::Complete, 0, false, query_info.settings_limit_offset_done);

    if (context->getSettingsRef()[Setting::allow_experimental_analyzer])
    {
        InterpreterSelectQueryAnalyzer interpreter(current_inner_query, getViewContext(context, storage_snapshot), options, column_names);
        interpreter.addStorageLimits(*query_info.storage_limits);
        query_plan = std::move(interpreter).extractQueryPlan();
    }
    else
    {
        InterpreterSelectWithUnionQuery interpreter(current_inner_query, getViewContext(context, storage_snapshot), options, column_names);
        interpreter.addStorageLimits(*query_info.storage_limits);
        interpreter.buildQueryPlan(query_plan);
    }

    /// It's expected that the columns read from storage are not constant.
    /// Because method 'getSampleBlockForColumns' is used to obtain a structure of result in InterpreterSelectQuery.
    ActionsDAG materializing_actions(query_plan.getCurrentHeader().getColumnsWithTypeAndName());
    materializing_actions.addMaterializingOutputActions(/*materialize_sparse=*/ true);

    auto materializing = std::make_unique<ExpressionStep>(query_plan.getCurrentHeader(), std::move(materializing_actions));
    materializing->setStepDescription("Materialize constants after VIEW subquery");
    query_plan.addStep(std::move(materializing));

    /// And also convert to expected structure.
    const auto & expected_header = storage_snapshot->getSampleBlockForColumns(column_names);
    const auto & header = query_plan.getCurrentHeader();

    const auto * select_with_union = current_inner_query->as<ASTSelectWithUnionQuery>();
    if (select_with_union && hasJoin(*select_with_union) && changedNullabilityOneWay(header, expected_header))
    {
        throw DB::Exception(ErrorCodes::INCORRECT_QUERY,
                            "Query from view {} returned Nullable column having not Nullable type in structure. "
                            "If query from view has JOIN, it may be cause by different values of 'join_use_nulls' setting. "
                            "You may explicitly specify 'join_use_nulls' in 'CREATE VIEW' query to avoid this error",
                            getStorageID().getFullTableName());
    }

    auto convert_actions_dag = ActionsDAG::makeConvertingActions(
            header.getColumnsWithTypeAndName(),
            expected_header.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name);

    auto converting = std::make_unique<ExpressionStep>(query_plan.getCurrentHeader(), std::move(convert_actions_dag));
    converting->setStepDescription("Convert VIEW subquery result to VIEW table structure");
    query_plan.addStep(std::move(converting));
}

static ASTTableExpression * getFirstTableExpression(ASTSelectQuery & select_query)
{
    if (!select_query.tables() || select_query.tables()->children.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No table expression in view select AST");

    auto * select_element = select_query.tables()->children[0]->as<ASTTablesInSelectQueryElement>();

    if (!select_element->table_expression)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Incorrect table expression");

    return select_element->table_expression->as<ASTTableExpression>();
}

void StorageView::replaceQueryParametersIfParametrizedView(ASTPtr & outer_query, const NameToNameMap & parameter_values)
{
    ReplaceQueryParameterVisitor visitor(parameter_values);
    visitor.visit(outer_query);
}

void StorageView::replaceWithSubquery(ASTSelectQuery & outer_query, ASTPtr view_query, ASTPtr & view_name, bool parameterized_view)
{
    ASTTableExpression * table_expression = getFirstTableExpression(outer_query);

    if (!table_expression->database_and_table_name)
    {
        /// If it's a view or merge table function, add a fake db.table name.
        /// For parameterized view, the function name is the db.view name, so add the function name
        if (table_expression->table_function)
        {
            auto table_function_name = table_expression->table_function->as<ASTFunction>()->name;
            if (table_function_name == "view" || table_function_name == "viewIfPermitted")
                table_expression->database_and_table_name = std::make_shared<ASTTableIdentifier>("__view");
            else if (table_function_name == "merge")
                table_expression->database_and_table_name = std::make_shared<ASTTableIdentifier>("__merge");
            else if (parameterized_view)
                table_expression->database_and_table_name = std::make_shared<ASTTableIdentifier>(table_function_name);

        }
        if (!table_expression->database_and_table_name)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Incorrect table expression");
    }

    DatabaseAndTableWithAlias db_table(table_expression->database_and_table_name);
    String alias = db_table.alias.empty() ? db_table.table : db_table.alias;

    view_name = table_expression->database_and_table_name;
    table_expression->database_and_table_name = {};
    table_expression->subquery = std::make_shared<ASTSubquery>(view_query);
    table_expression->subquery->setAlias(alias);

    for (auto & child : table_expression->children)
        if (child.get() == view_name.get())
            child = view_query;
        else if (child.get()
                 && child->as<ASTFunction>()
                 && table_expression->table_function
                 && table_expression->table_function->as<ASTFunction>()
                 && child->as<ASTFunction>()->name == table_expression->table_function->as<ASTFunction>()->name)
            child = view_query;
}

ASTPtr StorageView::restoreViewName(ASTSelectQuery & select_query, const ASTPtr & view_name)
{
    ASTTableExpression * table_expression = getFirstTableExpression(select_query);

    if (!table_expression->subquery)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Incorrect table expression");

    ASTPtr subquery = table_expression->subquery;
    table_expression->subquery = {};
    table_expression->database_and_table_name = view_name;

    for (auto & child : table_expression->children)
        if (child.get() == subquery.get())
            child = view_name;
    return subquery->children[0];
}

void StorageView::checkAlterIsPossible(const AlterCommands & commands, ContextPtr /* local_context */) const
{
    for (const auto & command : commands)
    {
        if (!command.isCommentAlter() && command.type != AlterCommand::MODIFY_SQL_SECURITY)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alter of type '{}' is not supported by storage {}", command.type, getName());
    }
}

void registerStorageView(StorageFactory & factory)
{
    factory.registerStorage("View", [](const StorageFactory::Arguments & args)
    {
        if (args.query.storage)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Specifying ENGINE is not allowed for a View");

        return std::make_shared<StorageView>(args.table_id, args.query, args.columns, args.comment);
    });
}

}
