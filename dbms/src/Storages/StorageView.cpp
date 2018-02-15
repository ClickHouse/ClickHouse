#include <Interpreters/InterpreterSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Common/typeid_cast.h>

#include <Storages/StorageView.h>
#include <Storages/StorageFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
}


StorageView::StorageView(
    const String & table_name_,
    const String & database_name_,
    const ASTCreateQuery & query,
    const NamesAndTypesList & columns_,
    const NamesAndTypesList & materialized_columns_,
    const NamesAndTypesList & alias_columns_,
    const ColumnDefaults & column_defaults_)
    : IStorage{columns_, materialized_columns_, alias_columns_, column_defaults_}, table_name(table_name_),
    database_name(database_name_)
{
    if (!query.select)
        throw Exception("SELECT query is not specified for " + getName(), ErrorCodes::INCORRECT_QUERY);

    inner_query = query.select->ptr();
}


BlockInputStreams StorageView::read(
    const Names & column_names,
    const SelectQueryInfo & /*query_info*/,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    processed_stage = QueryProcessingStage::FetchColumns;
    return InterpreterSelectQuery(inner_query->clone(), context, column_names).executeWithoutUnion();
}


void registerStorageView(StorageFactory & factory)
{
    factory.registerStorage("View", [](const StorageFactory::Arguments & args)
    {
        if (args.query.storage)
            throw Exception("Specifying ENGINE is not allowed for a View", ErrorCodes::INCORRECT_QUERY);

        return StorageView::create(
            args.table_name, args.database_name, args.query, args.columns,
            args.materialized_columns, args.alias_columns, args.column_defaults);
    });
}

}
