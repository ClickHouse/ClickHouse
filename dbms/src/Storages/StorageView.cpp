#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSubquery.h>

#include <Storages/StorageView.h>
#include <Storages/StorageFactory.h>

#include <DataStreams/MaterializingBlockInputStream.h>

#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
}


StorageView::StorageView(
    const String & table_name_,
    const ASTCreateQuery & query,
    const ColumnsDescription & columns_)
    : IStorage{columns_}, table_name(table_name_)
{
    if (!query.select)
        throw Exception("SELECT query is not specified for " + getName(), ErrorCodes::INCORRECT_QUERY);

    inner_query = query.select->ptr();
}


BlockInputStreams StorageView::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum processed_stage,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    checkQueryProcessingStage(processed_stage, context);

    BlockInputStreams res;

    if (context.getSettings().enable_optimize_predicate_expression)
    {
        replaceTableNameWithSubquery(typeid_cast<ASTSelectQuery *>(query_info.query.get()), inner_query);
        auto res_io = InterpreterSelectQuery(query_info.query, context, column_names, processed_stage).execute();

        res.emplace_back(res_io.in);
        return res;
    }

    res = InterpreterSelectWithUnionQuery(inner_query, context, column_names).executeWithMultipleStreams();

    /// It's expected that the columns read from storage are not constant.
    /// Because method 'getSampleBlockForColumns' is used to obtain a structure of result in InterpreterSelectQuery.
    for (auto & stream : res)
        stream = std::make_shared<MaterializingBlockInputStream>(stream);

    return res;
}

void StorageView::replaceTableNameWithSubquery(ASTSelectQuery * select_query, ASTPtr & subquery)
{
    ASTTablesInSelectQueryElement * select_element = static_cast<ASTTablesInSelectQueryElement *>(select_query->tables->children[0].get());

    if (!select_element->table_expression)
        throw Exception("Logical error: incorrect table expression", ErrorCodes::LOGICAL_ERROR);

    ASTTableExpression * table_expression = static_cast<ASTTableExpression *>(select_element->table_expression.get());

    if (!table_expression->database_and_table_name)
        throw Exception("Logical error: incorrect table expression", ErrorCodes::LOGICAL_ERROR);

    table_expression->database_and_table_name = {};
    table_expression->subquery = std::make_shared<ASTSubquery>();
    table_expression->subquery->children.push_back(subquery->clone());
}


void registerStorageView(StorageFactory & factory)
{
    factory.registerStorage("View", [](const StorageFactory::Arguments & args)
    {
        if (args.query.storage)
            throw Exception("Specifying ENGINE is not allowed for a View", ErrorCodes::INCORRECT_QUERY);

        return StorageView::create(args.table_name, args.query, args.columns);
    });
}

}
