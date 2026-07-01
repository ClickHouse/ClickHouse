#include <Interpreters/Context.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

#include <Storages/StorageObfuscate.h>
#include <Storages/StorageFactory.h>
#include <Storages/SelectQueryDescription.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageSnapshot.h>

#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ObfuscateStep.h>

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
        throw Exception(ErrorCodes::INCORRECT_QUERY, "SELECT query is not specified for {}", getName());
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
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected optimized VIEW query");
        current_inner_query = query_info.view_query->clone();
    }

    auto output_header = std::make_shared<const Block>(storage_snapshot->getSampleBlockForColumns(column_names));

    auto obfuscation = std::make_unique<ObfuscateStep>(std::move(output_header), current_inner_query, column_names, context);
    query_plan.addStep(std::move(obfuscation));
}

void registerStorageObfuscate(StorageFactory & factory);
void registerStorageObfuscate(StorageFactory & factory)
{
    factory.registerStorage("Obfuscate", [](const StorageFactory::Arguments & args)
    {
        if (args.query.storage)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Specifying ENGINE is not allowed for an Obfuscate");

        return std::make_shared<StorageObfuscate>(args.table_id, args.query, args.columns, args.comment);
    });
}

}
