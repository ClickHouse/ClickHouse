#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/PredicateExpressionsOptimizer.h>
#include <Interpreters/Context.h>
#include <Interpreters/getTableExpressions.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/queryToString.h>

#include <Storages/StorageView.h>
#include <Storages/StorageFactory.h>

#include <Common/typeid_cast.h>

#include <Processors/Pipe.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/Transforms/ConvertingTransform.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <DataStreams/ConvertingBlockInputStream.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
    extern const int ALIAS_REQUIRED;
}


StorageView::StorageView(
    const StorageID & table_id_,
    const ASTCreateQuery & query,
    const ColumnsDescription & columns_)
    : IStorage(table_id_)
{
    setColumns(columns_);

    if (!query.select)
        throw Exception("SELECT query is not specified for " + getName(), ErrorCodes::INCORRECT_QUERY);

    inner_query = query.select->ptr();
}


Pipes StorageView::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    Pipes pipes;

    ASTPtr current_inner_query = inner_query;

    if (context.getSettings().enable_optimize_predicate_expression)
        current_inner_query = getRuntimeViewQuery(*query_info.query->as<const ASTSelectQuery>(), context);

    InterpreterSelectWithUnionQuery interpreter(current_inner_query, context, {}, column_names);
    /// FIXME res may implicitly use some objects owned be pipeline, but them will be destructed after return
    if (query_info.force_tree_shaped_pipeline)
    {
        QueryPipeline pipeline;
        BlockInputStreams streams = interpreter.executeWithMultipleStreams(pipeline);

        for (auto & stream : streams)
        {
            stream = std::make_shared<MaterializingBlockInputStream>(stream);
            stream = std::make_shared<ConvertingBlockInputStream>(stream, getSampleBlockForColumns(column_names),
                                                                  ConvertingBlockInputStream::MatchColumnsMode::Name);
        }

        for (auto & stream : streams)
            pipes.emplace_back(std::make_shared<SourceFromInputStream>(std::move(stream)));
    }
    else
    {
        auto pipeline = interpreter.executeWithProcessors();

        /// It's expected that the columns read from storage are not constant.
        /// Because method 'getSampleBlockForColumns' is used to obtain a structure of result in InterpreterSelectQuery.
        pipeline.addSimpleTransform([](const Block & header)
        {
            return std::make_shared<MaterializingTransform>(header);
        });

        /// And also convert to expected structure.
        pipeline.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<ConvertingTransform>(header, getSampleBlockForColumns(column_names),
                                                         ConvertingTransform::MatchColumnsMode::Name);
        });

        pipes = std::move(pipeline).getPipes();
    }

    return pipes;
}

ASTPtr StorageView::getRuntimeViewQuery(const ASTSelectQuery & outer_query, const Context & context)
{
    auto temp_outer_query = outer_query.clone();
    auto * new_outer_select = temp_outer_query->as<ASTSelectQuery>();
    return getRuntimeViewQuery(new_outer_select, context, false);
}


static void replaceTableNameWithSubquery(ASTSelectQuery * select_query, ASTPtr & subquery)
{
    auto * select_element = select_query->tables()->children[0]->as<ASTTablesInSelectQueryElement>();

    if (!select_element->table_expression)
        throw Exception("Logical error: incorrect table expression", ErrorCodes::LOGICAL_ERROR);

    auto * table_expression = select_element->table_expression->as<ASTTableExpression>();

    if (!table_expression->database_and_table_name)
        throw Exception("Logical error: incorrect table expression", ErrorCodes::LOGICAL_ERROR);

    const auto alias = table_expression->database_and_table_name->tryGetAlias();
    table_expression->database_and_table_name = {};
    table_expression->subquery = std::make_shared<ASTSubquery>();
    table_expression->subquery->children.push_back(subquery);
    table_expression->children.push_back(table_expression->subquery);
    if (!alias.empty())
        table_expression->subquery->setAlias(alias);
}


ASTPtr StorageView::getRuntimeViewQuery(ASTSelectQuery * outer_query, const Context & context, bool normalize)
{
    auto runtime_view_query = inner_query->clone();

    /// TODO: remove getTableExpressions and getTablesWithColumns
    {
        const auto & table_expressions = getTableExpressions(*outer_query);
        const auto & tables_with_columns = getDatabaseAndTablesWithColumnNames(table_expressions, context);

        replaceTableNameWithSubquery(outer_query, runtime_view_query);
        if (context.getSettingsRef().joined_subquery_requires_alias && tables_with_columns.size() > 1)
        {
            for (const auto & pr : tables_with_columns)
                if (pr.table.table.empty() && pr.table.alias.empty())
                    throw Exception("Not unique subquery in FROM requires an alias (or joined_subquery_requires_alias=0 to disable restriction).",
                                    ErrorCodes::ALIAS_REQUIRED);
        }

        if (PredicateExpressionsOptimizer(context, tables_with_columns, context.getSettings()).optimize(*outer_query) && normalize)
            InterpreterSelectWithUnionQuery(
                runtime_view_query, context, SelectQueryOptions(QueryProcessingStage::FetchColumns).analyze().modify(), {});
    }

    return runtime_view_query;
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
