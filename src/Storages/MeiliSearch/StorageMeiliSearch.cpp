#include <Storages/StorageFactory.h>
#include <Storages/MeiliSearch/StorageMeiliSearch.h>
#include <Storages/IStorage.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/parseAddress.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/MeiliSearch/MeiliSearchConnection.h>
#include <Storages/MeiliSearch/SourceMeiliSearch.h>
#include <iostream>

namespace DB 
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

StorageMeiliSearch::StorageMeiliSearch(
        const StorageID & table_id,
        const MeiliSearchConfiguration& config_,
        const ColumnsDescription &  columns_,
        const ConstraintsDescription &  constraints_,
        const String &  comment) 
        : IStorage(table_id)
        , config{config_} {
            std::cout << "MeiliSearch table created " << 
                config.connection_string << " KEY = " << config.key << std::endl;
            StorageInMemoryMetadata storage_metadata;
            storage_metadata.setColumns(columns_);
            storage_metadata.setConstraints(constraints_);
            storage_metadata.setComment(comment);
            setInMemoryMetadata(storage_metadata);
        }


Pipe StorageMeiliSearch::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned)
{
    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());

    std::set<std::string> ans;
    std::cout << query_info.query->getQueryKindString() << "\n";
    query_info.query->collectIdentifierNames(ans);
    std::cout << "IDENTIFIERS\n";
    for (const auto& el : ans) {
        std::cout << el << "\n";
    }

    Block sample_block;
    for (const String & column_name : column_names)
    {
        auto column_data = metadata_snapshot->getColumns().getPhysical(column_name);
        sample_block.insert({ column_data.type, column_data.name });
    }

    return Pipe(std::make_shared<MeiliSearchSource>(config, sample_block, max_block_size, 0));
}

MeiliSearchConfiguration getConfiguration(ASTs engine_args) 
{

    if (engine_args.size() != 3) {
        throw Exception(
                "Storage MeiliSearch requires 3 parameters: MeiliSearch('url/host:port', index, key).",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

    // 7700 - default port
    try {
        auto parsed_host_port = parseAddress(engine_args[0]->as<ASTLiteral &>().value.safeGet<String>(), 7700);

        String host = parsed_host_port.first;
        UInt16 port = parsed_host_port.second;
        String index = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
        String key = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
        
        return MeiliSearchConfiguration(host, port, index, key);
    } catch(...) {
        String url = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();
        String index = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
        String key = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
        
        return MeiliSearchConfiguration(url, index, key);
    }
}

void registerStorageMeiliSearch(StorageFactory & factory)
{
    factory.registerStorage("MeiliSearch", [](const StorageFactory::Arguments & args) 
    {

        auto config = getConfiguration(args.engine_args);

        return StorageMeiliSearch::create(
            args.table_id,
            config,
            args.columns,
            args.constraints,
            args.comment);
    },
    {
        .source_access_type = AccessType::MEILISEARCH,
    });
}


}

