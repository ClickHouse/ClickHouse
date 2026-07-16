#include <Storages/BigQuery/StorageBigQuery.h>

#include <Core/Names.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <Interpreters/Context.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Processors/ISource.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/BigQuery/BigQueryClient.h>
#include <Storages/BigQuery/BigQueryConversions.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/Exception.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>

#include <fmt/ranges.h>

#include <ranges>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
    extern const int NOT_IMPLEMENTED;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
}

namespace
{

/// The recommended maximum number of rows per `tabledata.insertAll` request.
constexpr size_t INSERT_ALL_BATCH_SIZE = 500;

class BigQuerySource : public ISource
{
public:
    BigQuerySource(
        std::shared_ptr<BigQueryClient> client_,
        BigQueryFields fields_,
        SharedHeader sample_block,
        UInt64 max_block_size_,
        String selected_fields_)
        : ISource(sample_block)
        , client(std::move(client_))
        , fields(std::move(fields_))
        , max_block_size(max_block_size_)
        , selected_fields(std::move(selected_fields_))
    {
    }

    String getName() const override { return "BigQuery"; }

protected:
    Chunk generate() override
    {
        while (!all_read)
        {
            auto page = client->listTableData(page_token, selected_fields, max_block_size);

            if (is_first_page)
            {
                addTotalRowsApprox(page.total_rows);
                is_first_page = false;
            }

            page_token = page.next_page_token;
            if (page_token.empty())
                all_read = true;

            if (!page.rows || page.rows->size() == 0)
                continue;

            const auto & header = getPort().getHeader();
            MutableColumns columns = header.cloneEmptyColumns();

            for (size_t row = 0; row < page.rows->size(); ++row)
            {
                auto row_object = page.rows->getObject(static_cast<unsigned>(row));
                Poco::JSON::Array::Ptr cells;
                if (row_object)
                    cells = row_object->getArray("f");
                if (!cells || cells->size() != fields.size())
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed row in a BigQuery response");

                for (size_t i = 0; i < fields.size(); ++i)
                {
                    auto cell = cells->getObject(static_cast<unsigned>(i));
                    if (!cell)
                        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed row in a BigQuery response");
                    insertBigQueryValue(*columns[i], header.getByPosition(i).type, fields[i], cell->get("v"));
                }
            }

            size_t num_rows = columns.empty() ? 0 : columns[0]->size();
            return Chunk(std::move(columns), num_rows);
        }

        return {};
    }

private:
    std::shared_ptr<BigQueryClient> client;
    const BigQueryFields fields;
    const UInt64 max_block_size;
    const String selected_fields;

    String page_token;
    bool is_first_page = true;
    bool all_read = false;
};

class BigQuerySink : public SinkToStorage
{
public:
    BigQuerySink(std::shared_ptr<BigQueryClient> client_, BigQueryFields fields_, SharedHeader sample_block, String insert_id_prefix_)
        : SinkToStorage(sample_block)
        , client(std::move(client_))
        , fields(std::move(fields_))
        , header(sample_block)
        , insert_id_prefix(std::move(insert_id_prefix_))
    {
    }

    String getName() const override { return "BigQuerySink"; }

    void consume(Chunk & chunk) override
    {
        const auto & columns = chunk.getColumns();

        for (size_t row = 0; row < chunk.getNumRows(); ++row)
        {
            Poco::JSON::Object::Ptr row_json = new Poco::JSON::Object;
            for (size_t i = 0; i < fields.size(); ++i)
                row_json->set(fields[i].name, bigQueryJSONValue(fields[i], header->getByPosition(i).type, *columns[i], row));

            Poco::JSON::Object::Ptr entry = new Poco::JSON::Object;
            entry->set("json", row_json);

            /// A stable `insertId` (the query id plus a monotonic row ordinal) lets BigQuery
            /// best-effort deduplicate rows within its streaming-insert window. Streaming inserts are
            /// not atomic across `insertAll` batches, so if a later batch fails after earlier batches
            /// have been accepted, the earlier rows stay committed while the `INSERT` reports an error.
            /// With a stable `insertId`, both a transport-level retry of one batch and re-running the
            /// same `INSERT` (same query id, same input order) skip the already-committed rows instead
            /// of duplicating them. It is omitted when there is no query id (best-effort dedup off).
            if (!insert_id_prefix.empty())
                entry->set("insertId", insert_id_prefix + "-" + std::to_string(row_ordinal));
            ++row_ordinal;

            if (!pending_rows)
                pending_rows = new Poco::JSON::Array;
            pending_rows->add(entry);

            if (pending_rows->size() >= INSERT_ALL_BATCH_SIZE)
                flush();
        }
    }

    void onFinish() override
    {
        flush();
    }

private:
    void flush()
    {
        if (!pending_rows || pending_rows->size() == 0)
            return;
        client->insertAll(pending_rows);
        pending_rows = nullptr;
    }

    std::shared_ptr<BigQueryClient> client;
    const BigQueryFields fields;
    SharedHeader header;
    const String insert_id_prefix;
    size_t row_ordinal = 0;
    Poco::JSON::Array::Ptr pending_rows;
};

/// A BigQuery NULLABLE RECORD is inferred as `Nullable(Tuple(...))` so NULL records round-trip
/// losslessly. When declaring columns explicitly, a user may still prefer a plain `Tuple(...)` (which
/// coerces a whole-record NULL to a default tuple and avoids the `enable_nullable_tuple_type` setting),
/// or the exact `Nullable(Tuple(...))`. To accept both, treat a declared type that differs from the
/// inferred type only by `Nullable` wrappers placed directly around a `Tuple` as a match: this function
/// removes exactly those wrappers, and it is applied to both types before they are compared.
DataTypePtr stripNullableAroundTuple(const DataTypePtr & type)
{
    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(type.get()))
    {
        const auto & nested = nullable->getNestedType();
        if (typeid_cast<const DataTypeTuple *>(nested.get()))
            return stripNullableAroundTuple(nested);
        return std::make_shared<DataTypeNullable>(stripNullableAroundTuple(nested));
    }
    if (const auto * array = typeid_cast<const DataTypeArray *>(type.get()))
        return std::make_shared<DataTypeArray>(stripNullableAroundTuple(array->getNestedType()));
    if (const auto * tuple = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        DataTypes elements;
        elements.reserve(tuple->getElements().size());
        for (const auto & element : tuple->getElements())
            elements.push_back(stripNullableAroundTuple(element));
        return std::make_shared<DataTypeTuple>(elements, tuple->getElementNames());
    }
    return type;
}

/// The columns a user declared (in CREATE TABLE) or requested must match the BigQuery schema.
void checkColumnMatchesSchema(const NameAndTypePair & column, const BigQueryFields & fields)
{
    const auto * field = findBigQueryField(fields, column.name);
    if (!field)
        throw Exception(
            ErrorCodes::NO_SUCH_COLUMN_IN_TABLE,
            "There is no column '{}' in the BigQuery table. Columns of the BigQuery table: {}",
            column.name,
            fmt::join(std::ranges::views::transform(fields, [](const auto & f) { return f.name; }), ", "));

    /// Accept the exact inferred type, or one that differs only by Nullable-around-Tuple wrappers
    /// (see stripNullableAroundTuple) - e.g. a plain `Tuple` declared for an inferred `Nullable(Tuple)`.
    if (!column.type->equals(*field->data_type)
        && !stripNullableAroundTuple(column.type)->equals(*stripNullableAroundTuple(field->data_type)))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Column '{}' is declared as {}, but the BigQuery table schema maps it to {}. "
            "Declare the column with the exact type (conversions can be done in the query)",
            column.name, column.type->getName(), field->data_type->getName());
}

/// When the column list is inferred (no explicit `CREATE TABLE` structure), the types never
/// pass through the SQL-parser gate that `CREATE TABLE (col Nullable(Tuple(...)))` would go
/// through, so it must be checked explicitly here - otherwise, e.g., a `NULLABLE RECORD` field
/// would silently persist a `Nullable(Tuple(...))` column regardless of `enable_nullable_tuple_type`.
void validateInferredColumns(const ColumnsDescription & columns, const ContextPtr & context)
{
    DataTypeValidationSettings validation_settings(context->getSettingsRef());
    for (const auto & column : columns.getAllPhysical())
        validateDataType(column.type, validation_settings);
}

}

StorageBigQuery::StorageBigQuery(
    const StorageID & table_id_,
    BigQueryConfiguration configuration_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    ContextPtr context_,
    std::shared_ptr<BigQueryTokenProvider> token_provider_,
    std::optional<BigQueryFields> prefetched_fields_)
    : IStorage(table_id_)
    , configuration(std::move(configuration_))
    , token_provider(token_provider_ ? std::move(token_provider_) : std::make_shared<BigQueryTokenProvider>(configuration))
    , log(getLogger("StorageBigQuery (" + table_id_.getFullTableName() + ")"))
{
    StorageInMemoryMetadata storage_metadata;
    if (prefetched_fields_)
    {
        /// The `bigquery` table function already fetched the schema during analysis; reuse that snapshot
        /// so that analysis and execution see the same schema and no second `tables.get` is issued.
        storage_metadata.setColumns(columns_.empty() ? columnsDescriptionFromBigQuerySchema(*prefetched_fields_) : columns_);
        std::lock_guard lock(fields_mutex);
        fields = std::move(*prefetched_fields_);
    }
    else if (columns_.empty())
    {
        /// CREATE TABLE without a column list: infer the structure right away.
        auto schema = fetchTableSchema(configuration, context_, token_provider);
        auto inferred_columns = columnsDescriptionFromBigQuerySchema(schema);
        validateInferredColumns(inferred_columns, context_);
        storage_metadata.setColumns(inferred_columns);
        {
            std::lock_guard lock(fields_mutex);
            fields = std::move(schema);
        }
    }
    else
    {
        storage_metadata.setColumns(columns_);
    }
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
}

BigQueryFields StorageBigQuery::fetchTableSchema(
    const BigQueryConfiguration & configuration,
    ContextPtr context,
    const std::shared_ptr<BigQueryTokenProvider> & token_provider)
{
    auto table_object = token_provider
        ? BigQueryClient(configuration, context, token_provider).getTable()
        : BigQueryClient(configuration, context).getTable();

    /// tabledata.list works only for tables that have their own storage, not for views,
    /// materialized views or external tables.
    if (table_object->has("type"))
    {
        const auto type = table_object->getValue<String>("type");
        if (type == "VIEW" || type == "MATERIALIZED_VIEW" || type == "EXTERNAL")
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "BigQuery table '{}.{}' has type {} which cannot be read directly, only native tables are supported",
                configuration.dataset, configuration.table, type);
    }

    return parseBigQueryTableSchema(table_object);
}

const BigQueryFields & StorageBigQuery::getFields(ContextPtr query_context) const
{
    std::lock_guard lock(fields_mutex);
    if (!fields)
        fields = fetchTableSchema(configuration, query_context, token_provider);
    return *fields;
}

Pipe StorageBigQuery::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);

    const auto & all_fields = getFields(context);
    const auto & columns = storage_snapshot->metadata->getColumns();

    NameSet requested(column_names.begin(), column_names.end());

    /// The response returns the selected fields in the order of the table schema,
    /// so both the descriptors and the header follow that order.
    BigQueryFields selected;
    Block sample;
    for (const auto & field : all_fields)
    {
        if (!requested.contains(field.name))
            continue;
        auto column = columns.getPhysical(field.name);
        checkColumnMatchesSchema(column, all_fields);
        sample.insert(ColumnWithTypeAndName(column.type->createColumn(), column.type, column.name));
        selected.push_back(field);
    }

    for (const auto & name : column_names)
        if (!sample.has(name))
            checkColumnMatchesSchema(columns.getPhysical(name), all_fields);    /// throws with a proper message

    /// Always send the explicit field list, even when every column is requested. An empty
    /// `selectedFields` tells BigQuery to return all of the table's *current* columns; if the remote
    /// table gained a column after the schema snapshot was taken (analysis time), execution would then
    /// receive wider rows than the header expects and fail with "Malformed row". Sending the exact
    /// analyzed field list pins execution to the same schema snapshot that analysis used.
    Names selected_names;
    selected_names.reserve(selected.size());
    for (const auto & field : selected)
        selected_names.push_back(field.name);
    String selected_fields = fmt::format("{}", fmt::join(selected_names, ","));

    auto client = std::make_shared<BigQueryClient>(configuration, context, token_provider);
    return Pipe(std::make_shared<BigQuerySource>(
        std::move(client),
        std::move(selected),
        std::make_shared<const Block>(std::move(sample)),
        max_block_size,
        std::move(selected_fields)));
}

SinkToStoragePtr StorageBigQuery::write(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr context,
    bool /*async_insert*/)
{
    const auto & all_fields = getFields(context);
    const auto sample_block = metadata_snapshot->getSampleBlock();

    BigQueryFields sink_fields;
    for (const auto & column : sample_block)
    {
        checkColumnMatchesSchema(NameAndTypePair{column.name, column.type}, all_fields);
        sink_fields.push_back(*findBigQueryField(all_fields, column.name));
    }

    auto client = std::make_shared<BigQueryClient>(configuration, context, token_provider);
    return std::make_shared<BigQuerySink>(
        std::move(client), std::move(sink_fields), std::make_shared<const Block>(sample_block), context->getCurrentQueryId());
}

BigQueryConfiguration StorageBigQuery::getConfiguration(ASTs & engine_args, ContextPtr context, const StorageID * table_id)
{
    return BigQueryConfiguration::fromArguments(engine_args, context, table_id);
}

void registerStorageBigQuery(StorageFactory & factory);
void registerStorageBigQuery(StorageFactory & factory)
{
    factory.registerStorage(
        "BigQuery",
        [](const StorageFactory::Arguments & args)
        {
            auto configuration = StorageBigQuery::getConfiguration(args.engine_args, args.getLocalContext(), &args.table_id);
            return std::make_shared<StorageBigQuery>(
                args.table_id, std::move(configuration), args.columns, args.constraints, args.comment, args.getLocalContext());
        },
        {
            .supports_schema_inference = true,
            .source_access_type = AccessTypeObjects::Source::BIGQUERY,
        },
        Documentation{
            .description = R"DOCS_MD(
Allows reading from and writing to a table in [Google BigQuery](https://cloud.google.com/bigquery) over the BigQuery REST API.

Reading uses the `tabledata.list` API (only native tables can be read, not views), writing uses streaming inserts (`tabledata.insertAll`).
The table structure can be omitted, in which case it is inferred from the BigQuery table schema.

Exactly one authentication method must be provided:
- `access_token` — a ready-made OAuth 2.0 access token (for example, from `gcloud auth print-access-token`);
- `service_account_key` — the content of a Google service account key file in JSON format;
- `client_id`, `client_secret` and `refresh_token` — an OAuth client with a refresh token, as in Application Default Credentials.
)DOCS_MD",
            .syntax = "ENGINE = BigQuery('project', 'dataset', 'table'[, 'access_token'][, key = value, ...])",
            .related = {"bigquery", "MySQL", "MongoDB"}});
}

}
