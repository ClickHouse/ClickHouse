#include <algorithm>
#include <memory>
#include <optional>
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
#include <Storages/Elasticsearch/ElasticsearchSettings.h>
#include <Storages/Elasticsearch/StorageElasticsearch.h>
#include <Storages/Elasticsearch/ElasticsearchClient.h>
#include <Storages/AlterCommands.h>
#include <Parsers/ASTSetQuery.h>
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
    extern const int INCORRECT_DATA;
    extern const int NOT_IMPLEMENTED;
}

class ElasticsearchSource : public ISource
{
public:
    ElasticsearchSource(
        std::shared_ptr<ElasticsearchClient> client_,
        SharedHeader sample_block,
        ContextPtr context,
        const String & json_column_name,
        bool fetch_source_)
        : ISource(sample_block)
        , json_pos(sample_block->findPositionByName(json_column_name))
        , id_pos(sample_block->findPositionByName("_id"))
        , index_pos(sample_block->findPositionByName("_index"))
        , fetch_source(fetch_source_)
        , format_settings(getFormatSettings(context))
        , client(std::move(client_))
    {
        if (!json_pos)
            return;

        const auto & json_column = sample_block->getByPosition(*json_pos);
        json_serialization = json_column.type->getDefaultSerialization();
        object_serialization = dynamic_cast<const SerializationObject *>(json_serialization.get());
        if (!object_serialization)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} of type {} has no object serialization", json_column.name, json_column.type->getName());
    }

    String getName() const override { return "Elasticsearch"; }

private:

    Chunk generate() override
    {
        if (!has_data)
            return {};

        auto response = client->searchIndex(fetch_source);

        const auto & header = getPort().getHeader();
        MutableColumns columns = header.cloneEmptyColumns();
        std::ostringstream source_stream; // STYLE_CHECK_ALLOW_STD_STRING_STREAM

        size_t num_rows = response->size();

        if (num_rows == 0)
            return {};

        for (unsigned int i = 0; i < num_rows; ++i)
        {
            auto obj = response->getObject(i);
            if (!obj)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Hit #{} is not an object", i);

            if (index_pos)
                columns[*index_pos]->insert(obj->getValue<String>("_index"));
            if (id_pos)
                columns[*id_pos]->insert(obj->getValue<String>("_id"));

            if (!json_pos)
                continue;

            auto source = obj->getObject("_source");
            if (!source)
            {
                columns[*json_pos]->insertDefault();
                continue;
            }

            source_stream.str({});
            source->stringify(source_stream);
            object_serialization->deserializeObject(*columns[*json_pos], source_stream.view(), format_settings);
        }

        if (num_rows < client->pageSize())
            has_data = false;

        return Chunk(std::move(columns), num_rows);
    }

    bool has_data = true;
    std::optional<size_t> json_pos;
    std::optional<size_t> id_pos;
    std::optional<size_t> index_pos;
    bool fetch_source;
    SerializationPtr json_serialization;
    const SerializationObject * object_serialization = nullptr;
    FormatSettings format_settings;
    std::shared_ptr<ElasticsearchClient> client;
};

StorageElasticsearch::StorageElasticsearch(
    const StorageID & table_id_,
    ElasticsearchConfiguration configuration_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment_,
    std::unique_ptr<ElasticsearchSettings> settings_)
    : IStorage(table_id_)
    , config(configuration_)
{
    StorageInMemoryMetadata storage_metadata;
    if (columns_.empty())
    {
        storage_metadata.setColumns(getDefaultColumns());
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment_);
    storage_metadata.setVirtuals(createVirtuals());
    storage_metadata.setSettingsChanges(settings_->getSettingsChangesQuery());
    setInMemoryMetadata(storage_metadata);
    setSettings(std::move(settings_));
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

    String json_column_name = storage_snapshot->metadata->getColumns().getOrdinary().front().name;
    auto json_column_options = GetColumnsOptions(GetColumnsOptions::Ordinary).withSubcolumns();
    bool fetch_source = std::ranges::any_of(column_names, [&](const auto & name)
    {
        return storage_snapshot->tryGetColumn(json_column_options, name).has_value();
    });

    auto client = std::make_shared<ElasticsearchClient>(config, context, *storage_settings.get());
    return Pipe(std::make_shared<ElasticsearchSource>(
        std::move(client),
        std::make_shared<Block>(std::move(sample_block)), context, json_column_name, fetch_source));
}

VirtualColumnsDescription StorageElasticsearch::createVirtuals()
{
    VirtualColumnsDescription virtual_columns;

    virtual_columns.addEphemeral("_id", std::make_shared<DataTypeString>(), "", VirtualsMaterializationPlace::Reader);
    virtual_columns.addEphemeral("_index", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Reader);

    return virtual_columns;
}

void StorageElasticsearch::alter(const AlterCommands & params, ContextPtr query_context, AlterLockHolder & holder, DDLGuardPtr & ddl_guard)
{
    IStorage::alter(params, query_context, holder, ddl_guard);
    auto new_metadata = getInMemoryMetadataPtr(query_context, false);
    if (new_metadata->settings_changes)
    {
        const auto & settings_changes = new_metadata->settings_changes->as<const ASTSetQuery &>();
        auto new_settings = std::make_unique<ElasticsearchSettings>();
        new_settings->applyChanges(settings_changes.changes);
        setSettings(std::move(new_settings));
    }
}

void StorageElasticsearch::checkAlterIsPossible(const AlterCommands & commands, ContextPtr /* context */) const
{
    for (const auto & command : commands)
    {
        if (!command.isCommentAlter() && !command.isSettingsAlter())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alter of type '{}' is not supported by storage {}", command.type, getName());

        if (command.type == AlterCommand::MODIFY_SETTING)
        {
            ElasticsearchSettings new_settings = *storage_settings.get();
            new_settings.applyChanges(command.settings_changes);
            new_settings.sanityCheck();
        }
    }
}

ColumnsDescription StorageElasticsearch::getDefaultColumns()
{
    NamesAndTypesList names_and_types;

    names_and_types.emplace_back("document", std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON));
    return ColumnsDescription(std::move(names_and_types));
}

void StorageElasticsearch::checkColumns(const ColumnsDescription & descriptions)
{
    if (!descriptions.hasOnlyOrdinary())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Elasticsearch table engine does not support MATERIALIZED, ALIAS or EPHEMERAL columns");

    for (const auto * reserved_name : {"_id", "_index"})
        if (descriptions.has(reserved_name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Column name '{}' is reserved for a virtual column of the Elasticsearch table engine", reserved_name);

    auto columns = descriptions.getOrdinary();
    if (columns.size() > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Elasticsearch table engine requires at most one column, got {}", columns.size());

    if (!columns.empty() && !isObject(columns.front().type))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Elasticsearch table engine requires the column type to be JSON, got {}", columns.front().type->getName());
}

void registerStorageElasticsearch(StorageFactory & factory);
void registerStorageElasticsearch(StorageFactory & factory)
{
    factory.registerStorage(
        "Elasticsearch",
        [](const StorageFactory::Arguments & args)
        {
            StorageElasticsearch::checkColumns(args.columns);

            auto settings = std::make_unique<ElasticsearchSettings>();
            settings->loadFromQuery(*args.storage_def);
            settings->sanityCheck();
            auto configuration = ElasticsearchConfiguration::fromArguments(args.engine_args, args.getLocalContext(), &args.table_id);
            return std::make_shared<StorageElasticsearch>(
                args.table_id, std::move(configuration), args.columns, args.constraints, args.comment, std::move(settings));
        },
        SecretArgumentsSpec{.positional_secret_slots = {3}, .secret_keys = {"password", "bearer_token", "api_key"}},
        {
            .supports_settings = true,
            .supports_schema_inference = true,
            .source_access_type = AccessTypeObjects::Source::ELASTICSEARCH,
            .has_builtin_setting_fn = ElasticsearchSettings::hasBuiltin,
        },
        Documentation{
            .description = R"DOCS_MD(Table Engine for Elasticsearch))DOCS_MD",
            .syntax ="",
            .related={}
        });
}
}
