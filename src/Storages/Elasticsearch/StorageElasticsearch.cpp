#include <memory>
#include <Columns/ColumnObject.h>
#include <Columns/IColumn.h>
#include <Columns/IColumn_fwd.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <Interpreters/Context_fwd.h>
#include <Columns/ColumnString.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/Elasticsearch/ElasticsearchConfiguration.h>
#include <Storages/Elasticsearch/StorageElasticsearch.h>
#include <Storages/Elasticsearch/ElasticsearchClient.h>
#include <Processors/ISource.h>
#include <Storages/IStorage.h>
#include <Core/Block.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <QueryPipeline/Pipe.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Storages/VirtualColumnsDescription.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <DataTypes/Serializations/SerializationObject.h>
#include <IO/ReadBufferFromString.h>
#include <Common/Exception.h>
#include <Common/Macros.h>
#include <Common/assert_cast.h>
#include <Common/Documentation.h>
#include <Storages/StorageFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
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
        , json_pos(setObjectColumnPos(sample_block))
        , json_type(sample_block->getByPosition(json_pos).type)
        , id_pos(sample_block->findPositionByName("_id"))
        , index_pos(sample_block->findPositionByName("_index"))
        , json_deserializer(json_type->getDefaultSerialization())
        , object_deserializer(dynamic_cast<const SerializationObject *>(json_deserializer.get()))
        , format_settings(getFormatSettings(context))
        , client(std::move(client_))
        , logger(getLogger("Elasticsearch"))
    {
    }

    String getName() const override { return "Elasticsearch"; }

private:

    size_t setObjectColumnPos(const SharedHeader & sample_block)
    {
        const auto & data_types = sample_block->getDataTypes();
        for (size_t i = 0; i < data_types.size(); ++i)
        {
            const auto & col_type = data_types[i]; 
            if (isObject(col_type))
                return i;
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No Object column present in the header");
    }

    Chunk generate() override
    {
        if (page_returned)
            return {};

        auto response = client->searchIndex();
        page_returned = true;

        const auto & header = getPort().getHeader();
        MutableColumns columns = header.cloneEmptyColumns();
        std::ostringstream source_stream;

        for (unsigned int i = 0; i < response->size(); ++i)
        {
            auto obj = response->getObject(i);
            if (!obj)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Hit #{} is not an object", i);

            source_stream.str({});
            if (index_pos)
                columns[*index_pos]->insert(obj->getValue<String>("_index"));
            if (id_pos)
                columns[*id_pos]->insert(obj->getValue<String>("_id"));

            auto source = obj->getObject("_source");

            if (!source)
            {
                columns[json_pos]->insertDefault();
                continue;
            }

            source->stringify(source_stream);
            LOG_TRACE(logger, "Hit #{}: ##################################################\n {}", i, source_stream.view());          

            object_deserializer->deserializeObject(*columns[json_pos], source_stream.view(), format_settings);
        }

        size_t num_rows = columns[json_pos]->size();

        return Chunk(std::move(columns), num_rows);
    }

    bool page_returned = false;
    size_t json_pos;
    DataTypePtr json_type;
    std::optional<size_t> id_pos;
    std::optional<size_t> index_pos;
    SerializationPtr json_deserializer;
    const SerializationObject * object_deserializer;
    FormatSettings format_settings;
    std::shared_ptr<ElasticsearchClient> client;
    LoggerPtr logger;
};

StorageElasticsearch::StorageElasticsearch(
    const StorageID & table_id_,
    ElasticsearchConfiguration configuration_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constaints_,
    const String & comment_)
    : IStorage(table_id_)
    , config(configuration_)
{
    StorageInMemoryMetadata storage_metadata;
    if (columns_.empty())
    {
        NamesAndTypesList names_and_types;

        names_and_types.emplace_back("document", std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON));
        ColumnsDescription json_column(std::move(names_and_types));
        storage_metadata.setColumns(json_column);
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setConstraints(constaints_);
    storage_metadata.setComment(comment_);
    storage_metadata.setVirtuals(createVirtuals());
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

    Block sample_block = storage_snapshot->getSampleBlockForColumns(column_names);

    auto client = std::make_shared<ElasticsearchClient>(config, context);
    return Pipe(std::make_shared<ElasticsearchSource>(
        std::move(client),
        std::make_shared<Block>(std::move(sample_block)), context));
}

VirtualColumnsDescription StorageElasticsearch::createVirtuals()
{
    VirtualColumnsDescription virtual_columns;

    virtual_columns.addEphemeral("_id", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Reader);
    virtual_columns.addEphemeral("_index", std::make_shared<DataTypeString>(), "", VirtualsMaterializationPlace::Reader);

    return virtual_columns;
}

void registerStorageElasticsearch(StorageFactory & factory);
void registerStorageElasticsearch(StorageFactory & factory)
{
    factory.registerStorage(
        "Elasticsearch",
        [](const StorageFactory::Arguments & args)
        {
            /// Check the column argument
            auto physical_columns = args.columns.getAllPhysical();
            
            if (physical_columns.size() > 1)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Elasticsearch requires not 1 column argument, got {}", physical_columns.size());
            
            if (!physical_columns.empty())
            {
                auto col_type = physical_columns.getTypes().front();
                if (!isObject(col_type))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Elasticsearch requres the column type to be JSON, got {}", col_type->getName());
            }

            auto configuration = StorageElasticsearch::getConfiguration(args.engine_args, args.getLocalContext());
            return std::make_shared<StorageElasticsearch>(
                args.table_id, std::move(configuration), args.columns, args.constraints, args.comment);
        },
        {
            .supports_schema_inference = true,
            .source_access_type = AccessTypeObjects::Source::ELASTICSEARCH,
        },
        Documentation{
            .description = R"DOCS_MD(Elasticsearch docs))DOCS_MD",
            .syntax ="",
            .related={}
        });
}
}
