#include <Storages/Elasticsearch/ElasticsearchConfiguration.h>
#include <Storages/Elasticsearch/StorageElasticsearch.h>
#include <Storages/Elasticsearch/ElasticsearchClient.h>
#include <Processors/ISource.h>
#include <Storages/IStorage.h>
#include <Core/Block.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <QueryPipeline/Pipe.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Storages/checkAndGetLiteralArgument.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

ElasticsearchConfiguration StorageElasticsearch::getConfiguration(ASTs & args, ContextPtr context)
{
    ElasticsearchConfiguration configuration;

    if (args.empty() || args.size() < 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Elasticsearch requires arguments");
    
    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    configuration.url = checkAndGetLiteralArgument<String>(args[0], "base_url");
    configuration.index = checkAndGetLiteralArgument<String>(args[1], "index");
    return configuration;
}

class ElasticsearchSource : public ISource
{
public:
    ElasticsearchSource(
        std::shared_ptr<ElasticsearchClient> client_,
        SharedHeader sample_block)
        : ISource(sample_block)
        , client(std::move(client_))
    {
    }

    String getName() const override { return "BigQuery"; }

private:

    Chunk generate() override
    {
        auto response = client->searchIndex();

        for (auto elem : *response)
        {
            
        }
        
    }

    std::shared_ptr<ElasticsearchClient> client;
};

StorageElasticsearch::StorageElasticsearch(
    const StorageID & table_id_,
    ElasticsearchConfiguration configuration_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constaints_,
    const String & comment_)
    : IStorage(table_id_)
    , config(configuration_)
    , log(getLogger("StorageElasticsearch (" + table_id_.getFullTableName() + ")"))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constaints_);
    storage_metadata.setComment(comment_);
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageElasticsearch::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);

    Block sample_block;

    for (const String & column_name : column_names)
    {
        auto column_data = storage_snapshot->metadata->getColumns().getPhysical(column_name);
        sample_block.insert({ column_data.type, column_data.name });
    }
    auto client = std::make_shared<ElasticsearchClient>(config);
    return Pipe(std::make_shared<ElasticsearchSource>(
        std::move(client),
        std::make_shared<Block>(std::move(sample_block))));
}
}
