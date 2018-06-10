#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Parsers/ASTCreateQuery.h>

#include <Storages/StorageView.h>
#include <Storages/StorageFactory.h>

#include <DataStreams/MaterializingBlockInputStream.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
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
    const SelectQueryInfo & /*query_info*/,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    processed_stage = QueryProcessingStage::FetchColumns;
    BlockInputStreams res = InterpreterSelectWithUnionQuery(inner_query, context, column_names).executeWithMultipleStreams();

    /// It's expected that the columns read from storage are not constant.
    /// Because method 'getSampleBlockForColumns' is used to obtain a structure of result in InterpreterSelectQuery.
    for (auto & stream : res)
        stream = std::make_shared<MaterializingBlockInputStream>(stream);

    return res;
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
