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
#include <Common/SipHash.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>

#include <fmt/ranges.h>

#include <ranges>
#include <sstream>
#include <string_view>

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

/// BigQuery's `tabledata.insertAll` rejects HTTP requests larger than 10 MB. Rows are buffered across
/// chunks, so the sink flushes by serialized request size in addition to the row-count cap. A single
/// row whose serialized entry plus the request envelope alone exceeds this cannot be split and is
/// refused with a clear error instead of letting BigQuery reject the whole request with an opaque
/// `invalid`. The documented limit is decimal (10,000,000 bytes), not 10 MiB, so budgeting against
/// `10 * 1024 * 1024` would still let bodies in the 10,000,001..10,485,760-byte range through to a
/// remote rejection.
constexpr size_t INSERT_ALL_MAX_REQUEST_BYTES = 10'000'000;

/// The `insertAll` request body is `{"kind":"bigquery#tableDataInsertAllRequest","rows":[<entry>,...]}`
/// (see `BigQueryClient::insertAll`); these are the fixed bytes around the rows array, and consecutive
/// rows are separated by one comma each. Budgeting against the full request body (envelope, rows and
/// commas) rather than a blanket sub-limit margin keeps otherwise valid single-row inserts in the 9-10 MB
/// range from being rejected locally even though BigQuery would accept them.
constexpr std::string_view INSERT_ALL_REQUEST_ENVELOPE = R"({"kind":"bigquery#tableDataInsertAllRequest","rows":[]})";
constexpr size_t INSERT_ALL_REQUEST_ENVELOPE_BYTES = INSERT_ALL_REQUEST_ENVELOPE.size();

/// BigQuery rejects `insertId` values longer than 128 bytes, but the ClickHouse `query_id` used as the
/// deduplication prefix is user-controllable and unbounded. Keep a short query id verbatim (so the
/// `insertId` stays human-readable and stable), and replace a long one with its fixed-length hash, which
/// is just as stable for a given query id. The row ordinal is appended to the returned prefix.
String boundedInsertIdPrefix(const String & query_id)
{
    if (query_id.empty())
        return {};
    /// Leave room for the "-<ordinal>" suffix: the ordinal is a `size_t`, at most 20 decimal digits.
    static constexpr size_t max_insert_id_length = 128;
    static constexpr size_t max_ordinal_suffix_length = 1 + 20;
    static constexpr size_t max_prefix_length = max_insert_id_length - max_ordinal_suffix_length;
    if (query_id.size() <= max_prefix_length)
        return query_id;
    /// `sipHash128String` returns 32 hex characters, well within `max_prefix_length`.
    return sipHash128String(query_id);
}

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
            /// not atomic: one `insertAll` request can commit some rows while rejecting the others with
            /// `insertErrors`, and a later batch can fail after earlier batches have been accepted;
            /// either way the committed rows stay in BigQuery while the `INSERT` reports an error.
            /// With a stable `insertId`, both a transport-level retry of one batch and re-running the
            /// same `INSERT` (same query id, same input order) skip the already-committed rows instead
            /// of duplicating them. It is omitted when there is no query id (best-effort dedup off).
            if (!insert_id_prefix.empty())
                entry->set("insertId", insert_id_prefix + "-" + std::to_string(row_ordinal));
            ++row_ordinal;

            /// The serialized size of this row, so the batch can be flushed before the request exceeds
            /// BigQuery's size limit. The measured size matches this row's contribution to the request
            /// body, which is stringified the same (compact) way in `BigQueryClient::insertAll`.
            std::ostringstream entry_stream;  // STYLE_CHECK_ALLOW_STD_STRING_STREAM
            entry->stringify(entry_stream);
            const size_t entry_size = static_cast<size_t>(entry_stream.tellp());
            /// A single row whose serialized entry plus the request envelope alone exceeds the limit
            /// cannot be split into a smaller request, so it is refused up front.
            if (INSERT_ALL_REQUEST_ENVELOPE_BYTES + entry_size > INSERT_ALL_MAX_REQUEST_BYTES)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "A single row of {} bytes exceeds the maximum BigQuery streaming-insert request size of {} bytes",
                    entry_size, INSERT_ALL_MAX_REQUEST_BYTES);

            /// Flush the buffered rows before adding one that would push the serialized request over the
            /// size limit. The request body is the fixed envelope, the buffered rows, this row, and one
            /// comma between each pair of rows (the pending rows already contribute one comma each once
            /// this row is appended).
            if (pending_rows)
            {
                const size_t projected_bytes
                    = INSERT_ALL_REQUEST_ENVELOPE_BYTES + pending_bytes + entry_size + pending_rows->size();
                if (projected_bytes > INSERT_ALL_MAX_REQUEST_BYTES)
                    flush();
            }

            if (!pending_rows)
                pending_rows = new Poco::JSON::Array;
            pending_rows->add(entry);
            pending_bytes += entry_size;

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
        pending_bytes = 0;
    }

    std::shared_ptr<BigQueryClient> client;
    const BigQueryFields fields;
    SharedHeader header;
    const String insert_id_prefix;
    size_t row_ordinal = 0;
    Poco::JSON::Array::Ptr pending_rows;
    size_t pending_bytes = 0;
};

/// A BigQuery NULLABLE RECORD is inferred as `Nullable(Tuple(...))` so NULL records round-trip
/// losslessly. When declaring columns explicitly, a user may still prefer a plain `Tuple(...)` (which
/// coerces a whole-record NULL to a default tuple and avoids the `enable_nullable_tuple_type` setting),
/// or keep the exact `Nullable(Tuple(...))`. This function decides whether a declared type is
/// compatible with an inferred one: types must be structurally identical, with a single relaxation -
/// at a RECORD node, the declared side may drop the `Nullable` that the inferred side has directly
/// around the `Tuple`. The relaxation is applied per node and only in that direction, so it can never
/// move nullability to a different nesting level (e.g. accept a declared `Tuple(inner Nullable(Tuple))`
/// for an inferred `Nullable(Tuple(inner Tuple))`), which would silently change NULL semantics.
bool declaredTypeMatchesInferred(const DataTypePtr & declared, const DataTypePtr & inferred)
{
    if (declared->equals(*inferred))
        return true;

    DataTypePtr declared_core = declared;
    DataTypePtr inferred_core = inferred;

    /// If the inferred type is a `Nullable(Tuple(...))` (a NULLABLE RECORD), the declared side may keep
    /// that `Nullable` (handled by the exact-match fast path above) or drop it (plain `Tuple(...)`).
    /// Peel a single record-`Nullable` from the inferred side and, when present, the matching one from
    /// the declared side. A `Nullable` on the declared side that the inferred side lacks is not peeled,
    /// so it will fail the comparison below - the source is not nullable at this node.
    if (const auto * inferred_nullable = typeid_cast<const DataTypeNullable *>(inferred.get());
        inferred_nullable && typeid_cast<const DataTypeTuple *>(inferred_nullable->getNestedType().get()))
    {
        inferred_core = inferred_nullable->getNestedType();
        if (const auto * declared_nullable = typeid_cast<const DataTypeNullable *>(declared.get());
            declared_nullable && typeid_cast<const DataTypeTuple *>(declared_nullable->getNestedType().get()))
            declared_core = declared_nullable->getNestedType();
    }

    /// Recurse structurally: arrays element-wise, tuples field-wise (arity and names must match).
    if (const auto * declared_array = typeid_cast<const DataTypeArray *>(declared_core.get()))
    {
        const auto * inferred_array = typeid_cast<const DataTypeArray *>(inferred_core.get());
        return inferred_array && declaredTypeMatchesInferred(declared_array->getNestedType(), inferred_array->getNestedType());
    }
    if (const auto * declared_tuple = typeid_cast<const DataTypeTuple *>(declared_core.get()))
    {
        const auto * inferred_tuple = typeid_cast<const DataTypeTuple *>(inferred_core.get());
        if (!inferred_tuple
            || declared_tuple->getElements().size() != inferred_tuple->getElements().size()
            || declared_tuple->getElementNames() != inferred_tuple->getElementNames())
            return false;
        for (size_t i = 0; i < declared_tuple->getElements().size(); ++i)
            if (!declaredTypeMatchesInferred(declared_tuple->getElements()[i], inferred_tuple->getElements()[i]))
                return false;
        return true;
    }

    /// Not a RECORD relaxation and not structurally recursible: require exact equality (already failed).
    return declared_core->equals(*inferred_core);
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

    /// Accept the exact inferred type, or one that differs only by a Nullable dropped directly around a
    /// RECORD's Tuple (see declaredTypeMatchesInferred) - e.g. a plain `Tuple` for an inferred `Nullable(Tuple)`.
    if (!declaredTypeMatchesInferred(column.type, field->data_type))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Column '{}' is declared as {}, but the BigQuery table schema maps it to {}. "
            "Declare the column with the exact type (conversions can be done in the query)",
            column.name, column.type->getName(), field->data_type->getName());
}

/// A column that maps to a BigQuery RANGE is read-only: `tabledata.insertAll` expects a `RANGE<T>`
/// value as a `{start, end}` object, but the schema maps RANGE to an opaque String and discards the
/// element subtype, so the request shape BigQuery expects cannot be reconstructed. Reject such
/// columns on writes (recursively, so a RANGE nested inside a RECORD is caught too).
void checkFieldIsWritable(const BigQueryField & field)
{
    if (field.type == BigQueryField::Type::Range)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Writing to a BigQuery RANGE column ('{}') is not supported; RANGE columns are read-only", field.name);
    for (const auto & child : field.children)
        checkFieldIsWritable(child);
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

const BigQueryFields & StorageBigQuery::getFields(ContextPtr query_context, bool & snapshot_is_live) const
{
    std::lock_guard lock(fields_mutex);
    snapshot_is_live = !fields;
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

    bool snapshot_is_live = false;
    const auto & all_fields = getFields(context, snapshot_is_live);
    const auto & columns = storage_snapshot->metadata->getColumns();

    NameSet requested(column_names.begin(), column_names.end());

    /// The `tabledata.list` response returns the selected columns positionally, in the order of the
    /// table's *current* schema (not in the order they are listed in `selectedFields`). We build the
    /// descriptors and the header in the analyzed-snapshot order; the pre-read schema-drift check below
    /// guarantees that this order still matches the current schema, so the positional mapping is correct.
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

    /// Build the explicit list of requested columns to send as `selectedFields` (see below for why the
    /// read is always pinned to this list).
    Names selected_names;
    selected_names.reserve(selected.size());
    for (const auto & field : selected)
        selected_names.push_back(field.name);
    String selected_fields = fmt::format("{}", fmt::join(selected_names, ","));

    auto client = std::make_shared<BigQueryClient>(configuration, context, token_provider);

    /// `selectedFields` is passed in the `tabledata.list` request URL. It pins the *set and names* of the
    /// columns the read returns: an empty `selectedFields` would instead return all of the table's *current*
    /// columns, and because the response is positional and carries no column names, a concurrent schema
    /// change that keeps the column count (e.g. a dropped column offset by an added one) would be read into
    /// the wrong columns without tripping the per-row cell-count check. It does not, however, pin the column
    /// *order*: the response cells always follow the current-schema order, so a concurrent reorder of
    /// same-named columns is caught separately by the pre-read schema-drift check below, not by
    /// `selectedFields`. BigQuery tables can have up to 10000 columns, so for a very wide read the explicit
    /// list can exceed the request URL / front-end length limit. Rather than drop the list (which would
    /// unpin the column set) and risk a silent misread, such a read is rejected: it must project a smaller
    /// set of columns whose explicit list fits the request URL.
    ///
    /// The limit is budgeted against the *full* encoded request URI, not the raw field list: the URI also
    /// carries the table path and the fixed `prettyPrint` / `formatOptions.useInt64Timestamp` / `maxResults`
    /// parameters, and the field list is percent-encoded (each `,` becomes `%2C`), so the wire length is
    /// larger than `selected_fields.size()`. This up-front check measures exactly the first page (the
    /// `pageToken` that BigQuery adds from the second page onward is not known until a page has been
    /// fetched) and rejects only a read whose *first* request already does not fit — no headroom is
    /// reserved for the token, because a single-page read never carries one and reserving would reject
    /// perfectly valid near-threshold reads. A later page whose real, opaque token pushes the URL over the
    /// limit is caught by the authoritative guard in `BigQueryClient::listTableData`, which validates the
    /// actual URL of every paginated request against the same `BigQueryClient::max_request_uri_length`.
    const size_t request_uri_length = client->tableDataRequestUriLength(selected_fields, max_block_size);
    if (request_uri_length > BigQueryClient::max_request_uri_length)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The list of selected BigQuery columns is too long: it makes the `tabledata.list` request URL "
            "{} bytes, over the {}-byte limit, while pinning the read to the analyzed schema; "
            "select fewer columns",
            request_uri_length,
            BigQueryClient::max_request_uri_length);

    /// Fail-close schema-drift check. `selectedFields` pins the *set and names* of the columns that come
    /// back, but the `tabledata.list` response is positional and ordered by the table's *current* schema
    /// (see `BigQuerySource::generate`), so a concurrent replacement of the remote table that keeps the same
    /// column names in a different order — or changes a column's type — would be decoded into the wrong
    /// ClickHouse columns without any error, silently swapping type-compatible values. The header and the
    /// descriptors were built from the schema snapshot taken at analysis time, which can be arbitrarily
    /// older than execution. Re-fetch the live schema now and verify that the requested columns still appear
    /// with the same names, types, and relative order as that snapshot; otherwise reject the query instead
    /// of returning silently mismapped data.
    ///
    /// This closes the (potentially long) window between query analysis and execution. It cannot close the
    /// whole window: the schema and the row data are fetched by separate REST requests, so a schema change
    /// between this check and the first data request, or between the pages of a paginated read, is still
    /// possible. That residual window is inherent to `tabledata.list` (a positional, name-less response with
    /// no way to pin the schema across the read) and is documented as a limitation; it is the same class of
    /// exposure as any other external-table engine reading a remote table that is concurrently altered.
    ///
    /// The window starts when the snapshot is taken, which is *not* always earlier than this point: table
    /// metadata persists only the mapped ClickHouse columns, so a persistent table takes its snapshot on the
    /// first read or write after `CREATE`, `ATTACH`, or a server restart, i.e. in this very call. Then the
    /// snapshot is the live schema, there is nothing older to compare it against, and a second `tables.get`
    /// would only re-fetch what we just read - so reuse the snapshot instead of paying for that request. The
    /// declared columns are still validated against it above, and the row data is still decoded from it, so
    /// a change made while the server was down is adopted rather than silently mismapped. Extending the
    /// guarantee across reloads would require persisting the BigQuery wire schema in the table metadata,
    /// which the explicit-column-list form of `ENGINE = BigQuery(...)` never captures in the first place.
    const auto current_fields = snapshot_is_live ? all_fields : fetchTableSchema(configuration, context, token_provider);
    BigQueryFields current_selected;
    current_selected.reserve(selected.size());
    for (const auto & field : current_fields)
        if (requested.contains(field.name))
            current_selected.push_back(field);

    /// The comparison is over the BigQuery schema nodes themselves (type, mode, precision and scale, and
    /// the RECORD children, recursively), not over the mapped ClickHouse types: distinct BigQuery types map
    /// to the same ClickHouse type (`STRING` and `BYTES` both map to `String`, `REQUIRED GEOGRAPHY` and
    /// `NULLABLE GEOGRAPHY` both map to `Geometry`), while `BigQuerySource` decodes the wire payload from
    /// the `BigQueryField` metadata, so a same-mapped-type drift would still be decoded with the wrong rules.
    bool schema_changed = current_selected.size() != selected.size();
    for (size_t i = 0; !schema_changed && i < selected.size(); ++i)
        schema_changed = !bigQueryFieldsIdentical(current_selected[i], selected[i]);
    if (schema_changed)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "The schema of the BigQuery table `{}.{}` changed between query analysis and execution: the "
            "requested columns no longer match the analyzed schema (their names, types, or order differ). "
            "The `tabledata.list` response is positional, so reading now could silently return mismatched "
            "columns; re-run the query",
            configuration.dataset, configuration.table);

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
    bool snapshot_is_live = false;
    const auto & analyzed_fields = getFields(context, snapshot_is_live);
    const auto sample_block = metadata_snapshot->getSampleBlock();

    /// Fail-close write-side schema-drift check, the counterpart of the pre-read check in `read`. The
    /// schema snapshot in `analyzed_fields` is fetched once and cached for the lifetime of the storage, so
    /// it can be arbitrarily older than this `INSERT`. Validating the written columns against that stale
    /// snapshot alone is not enough: `tabledata.insertAll` accepts a JSON representation that several
    /// BigQuery types share, so if the remote table is replaced with the same column names but a different
    /// type, the rows would be accepted and stored under the wrong type with no error. For example an
    /// analyzed `INTEGER` column is serialized as decimal text (see `bigQueryJSONValue`), so after the
    /// remote column becomes `STRING` the very same `"123"` payload keeps being accepted, silently storing
    /// strings for a column the local table still declares as `Int64`. Re-fetch the live schema and check
    /// the written columns against it, and against the analyzed snapshot, before streaming any row.
    ///
    /// As in `read`, when this very call established the snapshot (a persistent table on its first read or
    /// write after `CREATE`, `ATTACH`, or a server restart, because the metadata persists only the mapped
    /// ClickHouse columns) the snapshot is the live schema, so reuse it instead of issuing a second
    /// `tables.get` that would compare it against itself.
    const auto current_fields = snapshot_is_live ? analyzed_fields : fetchTableSchema(configuration, context, token_provider);

    /// A table defined with an explicit subset of the BigQuery columns can be read, but it cannot be
    /// written to when an omitted remote field is `REQUIRED` and has no `defaultValueExpression`: the
    /// sink never sends that field, and `tabledata.insertAll` rejects every such row. When the field
    /// does declare a default, streaming inserts fill it in for the omitted column, so the subset stays
    /// writable. Reject the doomed `INSERT` up front, before any request, instead of letting each batch
    /// fail with a per-row remote error.
    for (const auto & field : current_fields)
        if (field.required && !field.has_default && !sample_block.has(field.name))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Cannot write to the BigQuery table `{}.{}`: the remote field '{}' is `REQUIRED` without a default "
                "value expression but is not present in the table definition, and BigQuery streaming inserts "
                "(`tabledata.insertAll`) reject rows that omit such a field. Include the column in the table "
                "definition to write to this table",
                configuration.dataset, configuration.table, field.name);

    BigQueryFields sink_fields;
    for (const auto & column : sample_block)
    {
        /// The declared column must match the *live* remote type, not just the analyzed one.
        checkColumnMatchesSchema(NameAndTypePair{column.name, column.type}, current_fields);

        /// As on the read side, the analyzed and the live field are compared as BigQuery schema nodes
        /// (recursively, including the RECORD children), because `bigQueryJSONValue` encodes a value from
        /// that metadata: a `STRUCT<name STRING>` that became a `STRUCT<name BYTES>` keeps mapping to the
        /// same `Tuple(name String)` while the child would now be base64-encoded on the wire.
        const auto & field = *findBigQueryField(current_fields, column.name);
        const auto * analyzed_field = findBigQueryField(analyzed_fields, column.name);
        if (!analyzed_field || !bigQueryFieldsIdentical(*analyzed_field, field))
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "The schema of the BigQuery table `{}.{}` changed since it was analyzed: column '{}' no longer "
                "matches the analyzed schema. Writing now could store the rows under a different type; "
                "re-create the table or re-run the query",
                configuration.dataset, configuration.table, column.name);

        checkFieldIsWritable(field);
        sink_fields.push_back(field);
    }

    auto client = std::make_shared<BigQueryClient>(configuration, context, token_provider);
    return std::make_shared<BigQuerySink>(
        std::move(client), std::move(sink_fields), std::make_shared<const Block>(sample_block),
        boundedInsertIdPrefix(context->getCurrentQueryId()));
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
The `BigQuery` engine allows reading from and writing to a table in [Google BigQuery](https://cloud.google.com/bigquery), including public datasets.

Reading uses the BigQuery REST API (`tabledata.list`), so only native tables can be read (views, materialized views and external tables cannot). Writing uses streaming inserts (`tabledata.insertAll`), which requires billing to be enabled for the project.

Writes are not atomic: a large `INSERT` is sent in batches (at most 500 rows per request, and also split to stay under BigQuery's 10 MB request-size limit), a single request may itself partially succeed (BigQuery can commit some rows of a request while rejecting the others with `insertErrors`), and a later batch may be rejected after earlier batches were accepted — in both cases the already-committed rows stay in BigQuery while the query reports an error. Each row carries a stable `insertId` (derived from the query id and the row's ordinal position) so that BigQuery best-effort deduplicates retried rows; because the `insertId` depends on the ordinal position, re-running the same `INSERT` deduplicates only when it presents the rows in the same order (for example single-threaded, with `max_threads = 1` and `max_insert_threads = 1`). See the [`bigquery` table function limitations](/reference/functions/table-functions/bigquery#limitations) for details.

## Creating a table {#creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
[(
    name1 [type1],
    name2 [type2],
    ...
)]
ENGINE = BigQuery(project, dataset, table[, access_token][, key = value, ...])
```

The column list is optional: when omitted, the structure is inferred from the BigQuery table schema. When specified, the columns can be a subset of the BigQuery columns, and each column must be declared with the exact type the BigQuery schema maps to (see the [data type mapping](/reference/functions/table-functions/bigquery#data-type-mapping)). A table whose definition omits a `REQUIRED` BigQuery field without a default value expression can be read but not written to: BigQuery streaming inserts reject rows that omit such a field, so such an `INSERT` is rejected up front. When the omitted `REQUIRED` field declares a `defaultValueExpression`, BigQuery fills the default in and the table stays writable. A `NULLABLE` `RECORD` is mapped to `Nullable(Tuple(...))` so `NULL` records round-trip losslessly; creating such a table (whether the structure is inferred or declared explicitly) requires the `enable_nullable_tuple_type` setting, as for any `Nullable(Tuple)` column. When declaring columns explicitly, a `RECORD` field may instead be declared as a plain `Tuple(...)` to avoid the setting, at the cost of coercing a whole-record `NULL` to a default tuple; the only accepted difference from the inferred type is dropping a `Nullable` that wraps a `RECORD`'s `Tuple`, and only at that same record — the nullability cannot be moved to a different (inner or outer) record.

**Engine parameters**

- `project` — The Google Cloud project that owns the dataset.
- `dataset` — The dataset name.
- `table` — The table name.
- `access_token` — An OAuth 2.0 access token (optional positional argument).

The parameters can also be passed as a [named collection](/concepts/features/configuration/server-config/named-collections) with `key = value` overrides. See the [`bigquery` table function](/reference/functions/table-functions/bigquery#arguments) for the full list of keys and the description of the [authentication methods](/reference/functions/table-functions/bigquery#authentication). Exactly one authentication method must be provided; for a permanent table a `service_account_key` or a `refresh_token` is preferable to an `access_token`, because access tokens expire within an hour.

## Usage example {#usage-example}

```sql
CREATE TABLE shakespeare
ENGINE = BigQuery('bigquery-public-data', 'samples', 'shakespeare',
                  service_account_key = '{"type": "service_account", ...}');

SELECT word, word_count FROM shakespeare ORDER BY word_count DESC LIMIT 3;

CREATE TABLE events (id Int64, payload Nullable(String))
ENGINE = BigQuery('my-project', 'my_dataset', 'events',
                  service_account_key = '{"type": "service_account", ...}');

INSERT INTO events VALUES (1, 'started');
```

## Related {#related}

- [`bigquery` table function](/reference/functions/table-functions/bigquery)
)DOCS_MD",
            .syntax = "ENGINE = BigQuery('project', 'dataset', 'table'[, 'access_token'][, key = value, ...])",
            .related = {"bigquery", "MySQL", "MongoDB"}});
}

}
