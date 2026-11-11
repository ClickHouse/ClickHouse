#include <Analyzer/FunctionNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/Elasticsearch/ElasticsearchConfiguration.h>
#include <Storages/Elasticsearch/ElasticsearchSettings.h>
#include <Storages/Elasticsearch/StorageElasticsearch.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>

#include <algorithm>

namespace DB
{

namespace Setting
{
    extern const SettingsBool enable_elasticsearch;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{
class TableFunctionElasticsearch : public ITableFunction
{
public:
    static constexpr auto name = "elasticsearch";

    std::string getName() const override { return name; }

    bool hasStaticStructure() const override { return true; }

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;

    const char * getStorageEngineName() const override { return "Elasticsearch"; }

    const String & getFunctionURI() const override { return configuration->url; }

    String getUsedNamedCollectionName() const override { return configuration ? configuration->named_collection_name : String{}; }

    ColumnsDescription getActualTableStructure(ContextPtr /*context*/, bool is_insert_query) const override;

    VectorWithMemoryTracking<size_t> skipAnalysisForArguments(const QueryTreeNodePtr & query_node_table_function, ContextPtr context) const override;

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    std::shared_ptr<ElasticsearchConfiguration> configuration;
    ElasticsearchSettings settings_provided_in_call;
    ColumnsDescription structure_columns;
};

StoragePtr TableFunctionElasticsearch::executeImpl(
    const ASTPtr & /*ast_function*/,
    ContextPtr context,
    const String & table_name,
    ColumnsDescription cached_columns,
    bool is_insert_query) const
{
    auto columns = cached_columns.empty() ? getActualTableStructure(context, is_insert_query) : std::move(cached_columns);
    StorageElasticsearch::checkColumns(columns);
    auto es_settings_ptr = std::make_unique<ElasticsearchSettings>(settings_provided_in_call);
    auto storage = std::make_shared<StorageElasticsearch>(
        StorageID(getDatabaseName(), table_name), *configuration, columns, ConstraintsDescription(), String{}, std::move(es_settings_ptr));
    storage->startup();
    return storage;
}

ColumnsDescription TableFunctionElasticsearch::getActualTableStructure(ContextPtr /*context*/, bool /*is_insert_query*/) const
{
    if (!structure_columns.empty())
        return structure_columns;
    return StorageElasticsearch::getDefaultColumns();
}

VectorWithMemoryTracking<size_t> TableFunctionElasticsearch::skipAnalysisForArguments(const QueryTreeNodePtr & query_node_table_function, ContextPtr) const
{
    const auto & arguments = query_node_table_function->as<TableFunctionNode &>().getArguments().getNodes();

    VectorWithMemoryTracking<size_t> result;
    for (size_t i = 0; i < arguments.size(); ++i)
    {
        const auto * function_node = arguments[i]->as<FunctionNode>();
        if (function_node && function_node->getFunctionName() == "query")
            result.push_back(i);
    }
    return result;
}

void TableFunctionElasticsearch::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    if (context->hasQueryContext() && !context->getSettingsRef()[Setting::enable_elasticsearch])
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "The elasticsearch table function is experimental. Set `enable_elasticsearch` setting to enable it");

    const auto & func_args = ast_function->as<ASTFunction &>();
    if (!func_args.arguments)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' must have arguments", getName());

    ASTs args = func_args.arguments->children;

    for (auto it = args.begin(); it != args.end(); ++it)
    {
        const ASTSetQuery * settings_ast = (*it)->as<ASTSetQuery>();
        if (settings_ast)
        {
            settings_provided_in_call.loadFromQuery(*settings_ast);
            settings_provided_in_call.sanityCheck();
            args.erase(it);
            break;
        }
    }

    for (auto it = args.begin(); it != args.end(); ++it)
    {
        const ASTFunction * func_ast = (*it)->as<ASTFunction>();
        if (!func_ast || func_ast->name != "equals" || !func_ast->arguments || func_ast->arguments->children.size() != 2)
            continue;

        if (tryGetIdentifierName(func_ast->arguments->children[0]) != "structure")
            continue;

        auto structure = checkAndGetLiteralArgument<String>(func_ast->arguments->children[1], "structure");
        structure_columns = parseColumnsListFromString(structure, context);
        StorageElasticsearch::checkColumns(structure_columns);
        args.erase(it);
        break;
    }

    const ASTs unevaluated_args = args;
    configuration = std::make_shared<ElasticsearchConfiguration>(ElasticsearchConfiguration::fromArguments(args, context));

    auto & persisted_args = func_args.arguments->children;
    for (size_t i = 0; i < args.size(); ++i)
        std::ranges::replace(persisted_args, unevaluated_args[i], args[i]);
}

}

void registerTableFunctionElasticsearch(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionElasticsearch>(
        {
            .description = R"DOCS_MD(
import { ExperimentalBadge } from "/snippets/components/ExperimentalBadge/ExperimentalBadge.jsx";

<ExperimentalBadge/>

Allows `SELECT` queries on documents of an index in [Elasticsearch](https://www.elastic.co/elasticsearch). Each document is returned as one row: its `_source` is exposed as a single column of the [JSON](/reference/data-types/newjson) type, and the document id and index name are available as virtual columns. The function is read-only.

## Syntax {#syntax}

```sql
elasticsearch('url', 'index'[, 'user', 'password'][, query('...')][, key = value, ...][, structure = 'document JSON'][, SETTINGS ...])
elasticsearch(named_collection[, query('...')][, key = value, ...][, structure = 'document JSON'][, SETTINGS ...])
```

<Note>
The Elasticsearch integration is experimental. Enable it with the [`enable_elasticsearch`](/reference/settings/session-settings/enable#enable_elasticsearch) setting:

```sql
SET enable_elasticsearch = 1;
```
</Note>

## Arguments {#arguments}

| Argument   | Description |
|------------|-------------|
| `url`      | The base URL of the Elasticsearch cluster, for example `https://localhost:9200`. It must be an `http` or `https` URL without credentials, query string or fragment. |
| `index`    | The index to read. It can also be an alias, a comma-separated list, or a wildcard pattern, as accepted by the Elasticsearch point in time API. A pattern that matches no index is an error. |
| `user`     | The user name for HTTP basic authentication (optional). |
| `password` | The password for HTTP basic authentication (optional, requires `user`). |
| `query('...')` | An Elasticsearch [Query DSL](https://www.elastic.co/docs/explore-analyze/query-filter/languages/querydsl) object in JSON that selects the documents to read (optional). It must follow the positional arguments. Default: `{"match_all": {}}`. |

The `url`, `index`, `user` and `password` arguments can also be given in the `key = value` form; positional arguments fill these slots in this order, and specifying an argument both positionally and as a key (or the same key twice) is an error.

The following arguments can be specified in the `key = value` form (or as keys of a named collection):

| Key            | Description |
|----------------|-------------|
| `user`         | The user name for HTTP basic authentication. |
| `password`     | The password for HTTP basic authentication. |
| `bearer_token` | A token sent as `Authorization: Bearer <token>`, for example an Elasticsearch service account token. |
| `api_key`      | An Elasticsearch API key, sent as `Authorization: ApiKey <api_key>`. The value is the base64-encoded `id:api_key` string returned by the create API key API. |

A named collection can contain the `query` key. To override it, pass `query('...')` or `query = '...'` after the collection name.

In addition, the table function accepts:

| Key         | Description |
|-------------|-------------|
| `structure` | The table structure in the form `'name JSON'`. The structure must contain exactly one column of type `JSON`; the names `_id` and `_index` are reserved. Default: `'document JSON'`. |

The read settings `keep_alive` and `page_size` can be passed in a trailing `SETTINGS` clause; see the [`Elasticsearch` table engine](/reference/engines/table-engines/integrations/elasticsearch#creating-a-table) for their description.

## Authentication {#authentication}

At most one authentication method can be provided:

1. **Basic authentication**. Pass `user` and `password`.
2. **Bearer token**. Pass `bearer_token`.
3. **API key**. Pass `api_key`.

Without any of them the request is sent anonymously, which only works when security is disabled in Elasticsearch.

Store credentials in a [named collection](/concepts/features/configuration/server-config/named-collections) to avoid specifying them in each query. A permanent table created from a named collection (with the `Elasticsearch` table engine or `CREATE TABLE ... AS elasticsearch(...)`) is registered as a dependency of the collection, so `DROP NAMED COLLECTION` is blocked while the table exists.

## Virtual columns {#virtual-columns}

- `_id` — The document id. Type: `String`.
- `_index` — The name of the index the document was read from. Type: `LowCardinality(String)`.

When a query reads only virtual columns, the document `_source` is not requested from Elasticsearch. An index that does not store `_source` (the `_source` field is disabled in its mapping) can be read only through the virtual columns: a query that reads the `JSON` column from such an index fails with an error instead of returning empty documents.

## Examples {#examples}

Count the documents of an index:

```sql
SELECT count()
FROM elasticsearch('https://localhost:9200', 'logs', 'elastic', 'changeme');
```

Read fields of the documents as subcolumns of the `JSON` column:

```sql
SELECT _id, document.timestamp, document.message
FROM elasticsearch('https://localhost:9200', 'logs', api_key = '...')
ORDER BY document.timestamp DESC
LIMIT 10;
```

Filter on the Elasticsearch side with a Query DSL object and tune the page size:

```sql
SELECT _id
FROM elasticsearch('https://localhost:9200', 'logs',
                   query('{"term": {"level": "error"}}'),
                   bearer_token = '...',
                   SETTINGS page_size = 1000);
```

Use a named collection:

```xml
<clickhouse>
    <named_collections>
        <my_elasticsearch>
            <url>https://localhost:9200</url>
            <user>elastic</user>
            <password>changeme</password>
        </my_elasticsearch>
    </named_collections>
</clickhouse>
```

```sql
SELECT * FROM elasticsearch(my_elasticsearch, index = 'logs');
```

## Limitations {#limitations}

- The function is read-only; `INSERT` is not supported.
- Predicates are not pushed down. A `WHERE` condition is applied in ClickHouse after the documents have been downloaded; use the `query` argument to filter on the Elasticsearch side.
- The `query` is sent with the `size` of a page. A query whose result depends on `size`, such as a `knn` query without `k`, returns a set of documents that depends on `page_size`. Specify such parameters explicitly.
- Each page is a separate search request, and Elasticsearch evaluates date math such as `now-1h` in the `query` anew for each request. The point in time freezes the documents but not the clock, so a time-dependent `query` can silently skip documents near the moving boundary. Use absolute dates in the `query`.
- Each read opens a point in time, pages through the results with `search_after` sorted by `_shard_doc`, and closes the point in time at the end. A page request that fails on any shard, or that times out in Elasticsearch, raises an error instead of returning partial results.
- Elasticsearch 7.12 or later is required: the function relies on the point in time API and on the `_shard_doc` sort field, so older versions and OpenSearch reject the first request.
- `page_size` cannot exceed the `index.max_result_window` index setting of Elasticsearch (`10000` by default).

## Related {#related}

- [`Elasticsearch` table engine](/reference/engines/table-engines/integrations/elasticsearch)
)DOCS_MD",
            .category = FunctionDocumentation::Category::TableFunction
        },
        elasticsearchSecretArguments());
}

}
