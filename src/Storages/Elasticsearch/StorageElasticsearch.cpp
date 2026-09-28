#include <Columns/ColumnObject.h>
#include <Columns/IColumn.h>
#include <Columns/IColumn_fwd.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <Interpreters/Context_fwd.h>
#include <Columns/ColumnString.h>
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
#include <DataTypes/Serializations/SerializationObject.h>
#include <IO/ReadBufferFromString.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int INCORRECT_DATA;
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
        SharedHeader sample_block,
        ContextPtr context)
        : ISource(sample_block)
        , client(std::move(client_))
        , json_type(sample_block->getDataTypes()[0])
        , json_deserializer(json_type->getDefaultSerialization())
        , object_deserializer(assert_cast<const SerializationObject *>(json_deserializer.get()))
        , format_settings(getFormatSettings(context))
    {
        if (!isObject(json_type))
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "ElasticsearchSource expects a JSON column, got {}", json_type->getName());
    }

    String getName() const override { return "Elasticsearch"; }

private:

    Chunk generate() override
    {
        if (finished)
            return {};

        auto response = client->searchIndex();

        auto index_column = ColumnString::create();
        auto id_column = ColumnString::create();

        auto document_column = json_type->createColumn();
        std::ostringstream source_stream;

        for (unsigned int i = 0; i < response->size(); ++i)
        {
            auto obj = response->getObject(i);
            if (!obj)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Hit #{} is not an object", i);

            source_stream.str({});
            index_column->insert(obj->getValue<String>("_index"));
            id_column->insert(obj->getValue<String>("_id"));

            auto source = obj->getObject("_source");

            if (!source)
            {
                document_column->insertDefault();
                continue;
            }

            source->stringify(source_stream);
            object_deserializer->deserializeObject(*document_column, source_stream.view(), format_settings);
        }

        size_t num_rows = id_column->size();
        MutableColumns columns;
        columns.emplace_back(std::move(id_column));
        columns.emplace_back(std::move(index_column));
        columns.emplace_back(std::move(document_column));

        return Chunk(std::move(columns), num_rows);
    }

    std::shared_ptr<ElasticsearchClient> client;
    DataTypePtr json_type;
    SerializationPtr json_deserializer;
    const SerializationObject * object_deserializer;
    FormatSettings format_settings;
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
    ContextPtr context,
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
    auto client = std::make_shared<ElasticsearchClient>(config, context);
    return Pipe(std::make_shared<ElasticsearchSource>(
        std::move(client),
        std::make_shared<Block>(std::move(sample_block)), context));
}
}
