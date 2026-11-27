#include <algorithm>
#include <memory>
#include <optional>
#include <Columns/ColumnObject.h>
#include <Columns/IColumn.h>
#include <Columns/IColumn_fwd.h>
#include <Core/NamesAndTypes.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <Interpreters/Context.h>
#include <Columns/ColumnString.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/Elasticsearch/ElasticsearchConfiguration.h>
#include <Storages/Elasticsearch/ElasticsearchSettings.h>
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
#include <IO/ReadBufferFromString.h>
#include <Common/Exception.h>
#include <Common/Macros.h>
#include <Common/assert_cast.h>
#include <Common/Documentation.h>
#include <Storages/StorageFactory.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool enable_elasticsearch;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
}

class ElasticsearchSource : public ISource
{
public:
    ElasticsearchSource(
        std::shared_ptr<ElasticsearchClient> client_,
        SharedHeader sample_block,
        const String & json_column_name)
        : ISource(sample_block)
        , client(std::move(client_))
    {
        hit_columns.index_pos = sample_block->findPositionByName("_index");
        hit_columns.id_pos = sample_block->findPositionByName("_id");
        hit_columns.source_pos = sample_block->findPositionByName(json_column_name);
        if (hit_columns.source_pos)
            hit_columns.source_serialization = sample_block->getByPosition(*hit_columns.source_pos).type->getDefaultSerialization();
    }

    String getName() const override { return "Elasticsearch"; }

private:

    Chunk generate() override
    {
        if (!has_data)
            return {};

        MutableColumns columns = getPort().getHeader().cloneEmptyColumns();

        auto response = client->searchIndex(hit_columns);
        while (response->readHit(columns))
        {
        }
        client->finishPage(*response);

        size_t num_rows = response->hitsRead();
        if (num_rows < client->pageSize())
            has_data = false;

        if (num_rows == 0)
            return {};

        return Chunk(std::move(columns), num_rows);
    }

    bool has_data = true;
    ElasticsearchHitColumns hit_columns;
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
    , storage_settings(std::move(settings_))
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
    storage_metadata.setSettingsChanges(storage_settings->getSettingsChangesQuery());
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

    String json_column_name = storage_snapshot->metadata->getColumns().getOrdinary().front().name;

    auto client = std::make_shared<ElasticsearchClient>(config, context, *storage_settings);
    return Pipe(std::make_shared<ElasticsearchSource>(
        std::move(client),
        std::make_shared<Block>(std::move(sample_block)), json_column_name));
}

VirtualColumnsDescription StorageElasticsearch::createVirtuals()
{
    VirtualColumnsDescription virtual_columns;

    virtual_columns.addEphemeral("_id", std::make_shared<DataTypeString>(), "", VirtualsMaterializationPlace::Reader);
    virtual_columns.addEphemeral("_index", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Reader);

    return virtual_columns;
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
            if (!isLoadingFromExistingMetadata(args.mode) && !args.getLocalContext()->getSettingsRef()[Setting::enable_elasticsearch])
                throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                    "The Elasticsearch table engine is experimental. Set `enable_elasticsearch` setting to enable it");

            StorageElasticsearch::checkColumns(args.columns);

            auto settings = std::make_unique<ElasticsearchSettings>();
            settings->loadFromQuery(*args.storage_def);
            settings->sanityCheck();
            auto configuration = ElasticsearchConfiguration::fromArguments(args.engine_args, args.getLocalContext(), &args.table_id);
            return std::make_shared<StorageElasticsearch>(
                args.table_id, std::move(configuration), args.columns, args.constraints, args.comment, std::move(settings));
        },
        elasticsearchSecretArguments(),
        {
            .supports_settings = true,
            .supports_schema_inference = true,
            .source_access_type = AccessTypeObjects::Source::ELASTICSEARCH,
            .has_builtin_setting_fn = ElasticsearchSettings::hasBuiltin,
        },
        Documentation{
            .description = R"DOCS_MD(
import { ExperimentalBadge } from "/snippets/components/ExperimentalBadge/ExperimentalBadge.jsx";

<ExperimentalBadge/>

The `Elasticsearch` engine allows reading documents from an index in [Elasticsearch](https://www.elastic.co/elasticsearch). Each document is returned as one row: its `_source` is exposed as a single column of the [JSON](/reference/data-types/newjson) type, and the document id and index name are available as virtual columns.

The engine is read-only. Documents are fetched with the `_search` API in pages, using a [point in time](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/paginate-search-results#search-after) so that a query sees a consistent snapshot of the index even while it is being modified.

Elasticsearch 7.12 or later is required: the engine relies on the point in time API and on the `_shard_doc` sort field. OpenSearch is not supported, because it has no `_shard_doc` field. An older server rejects the first request with an error.

## Creating a table {#creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
[(
    name1 JSON
)]
ENGINE = Elasticsearch('url', 'index'[, 'user', 'password'][, query('...')][, key = value, ...])
[SETTINGS keep_alive = 60, page_size = 10000]
```

<Note>
The Elasticsearch integration is experimental. Enable it with the [`enable_elasticsearch`](/reference/settings/session-settings/enable#enable_elasticsearch) setting:

```sql
SET enable_elasticsearch = 1;
```
</Note>

The column list is optional. When omitted, the table has a single column `document` of type `JSON`. When specified, it must contain exactly one column, and its type must be `JSON`, optionally with type hints and skip paths. `MATERIALIZED`, `ALIAS` and `EPHEMERAL` columns are not supported, and the names `_id` and `_index` are reserved for the virtual columns.

**Engine parameters**

- `url` — The base URL of the Elasticsearch cluster, for example `https://localhost:9200`. It must be an `http` or `https` URL without credentials, query string or fragment.
- `index` — The index to read. It can also be an alias, a data stream, a comma-separated list, a wildcard pattern, or a date math expression such as `<logs-{now/d}>`, as accepted by the Elasticsearch point in time API. The value is the literal name and must not be URL-encoded. A pattern that matches no index is an error.
- `user`, `password` — Optional credentials for HTTP basic authentication.
- `query('...')` — An optional Elasticsearch [Query DSL](https://www.elastic.co/docs/explore-analyze/query-filter/languages/querydsl) object in JSON that selects the documents to read. It must follow the positional arguments. Default: `{"match_all": {}}`.

The parameters can also be passed as a [named collection](/concepts/features/configuration/server-config/named-collections) with `key = value` overrides. See the [`elasticsearch` table function](/reference/functions/table-functions/elasticsearch#arguments) for the full list of keys and the description of the [authentication methods](/reference/functions/table-functions/elasticsearch#authentication).

**Engine settings**

| Setting      | Default | Description |
|--------------|---------|-------------|
| `keep_alive` | `60`    | How long, in seconds, Elasticsearch keeps the point in time open between two consecutive page requests. |
| `page_size`  | `10000` | The number of documents requested from Elasticsearch per request. Each page becomes one block, so this setting also sets the block size of the read; [`max_block_size`](/reference/settings/session-settings/max#max_block_size) has no effect. Values above the `index.max_result_window` index setting of Elasticsearch (`10000` by default) are rejected by Elasticsearch. |

The settings cannot be changed with `ALTER TABLE ... MODIFY SETTING`. To change them, recreate the table.

## Virtual columns {#virtual-columns}

- `_id` — The document id. Type: `String`.
- `_index` — The name of the index the document was read from. Type: `LowCardinality(String)`.

When a query reads only virtual columns, the document `_source` is not requested from Elasticsearch. An index that does not store `_source` (the `_source` field is disabled in its mapping) can be read only through the virtual columns: a query that reads the `JSON` column from such an index fails with an error instead of returning empty documents.

## Usage example {#usage-example}

```sql
CREATE TABLE logs
ENGINE = Elasticsearch('https://localhost:9200', 'logs-*', 'elastic', 'changeme')
SETTINGS page_size = 5000;

SELECT _index, _id, document.message
FROM logs
WHERE document.level = 'error'
LIMIT 10;
```

Filter on the Elasticsearch side with a Query DSL object:

```sql
CREATE TABLE errors_since_october
ENGINE = Elasticsearch('https://localhost:9200', 'logs-*',
                       query('{"bool": {"filter": [{"term": {"level": "error"}}, {"range": {"@timestamp": {"gte": "2026-10-01T00:00:00Z"}}}]}}'),
                       api_key = '...');
```

Use absolute dates in the `query`: date math such as `now-1h` is evaluated anew for each page, so it can skip documents. See the [limitations of the `elasticsearch` table function](/reference/functions/table-functions/elasticsearch#limitations).

## Related {#related}

- [`elasticsearch` table function](/reference/functions/table-functions/elasticsearch)
)DOCS_MD",
            .syntax ="",
            .related={}
        });
}
}
