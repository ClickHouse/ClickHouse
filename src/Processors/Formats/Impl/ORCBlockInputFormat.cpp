#include <Processors/Formats/Impl/ORCBlockInputFormat.h>
#include <Common/Exception.h>

#if USE_ORC
#    include <DataTypes/NestedUtils.h>
#    include <Formats/FormatFactory.h>
#    include <Formats/SchemaInferenceUtils.h>
#    include <IO/ReadBufferFromMemory.h>
#    include <IO/WriteHelpers.h>
#    include <IO/copyData.h>
#    include <boost/algorithm/string/case_conv.hpp>
#    include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#    include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#    include <Processors/Formats/Impl/NativeORCBlockInputFormat.h>
#    include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_READ_ALL_DATA;
}

ORCBlockInputFormat::ORCBlockInputFormat(ReadBuffer & in_, SharedHeader header_, const FormatSettings & format_settings_)
    : IInputFormat(std::move(header_), &in_)
    , block_missing_values(getPort().getHeader().columns())
    , format_settings(format_settings_)
    , skip_stripes(format_settings.orc.skip_stripes)
{
}

Chunk ORCBlockInputFormat::read()
{
    block_missing_values.clear();

    if (!file_reader)
        prepareReader();

    if (is_stopped)
        return {};

    for (; stripe_current < stripe_total && skip_stripes.contains(stripe_current); ++stripe_current)
        ;

    if (stripe_current >= stripe_total)
        return {};

    if (need_only_count)
        return getChunkForCount(file_reader->GetRawORCReader()->getStripe(stripe_current++)->getNumberOfRows());

    auto batch_result = file_reader->ReadStripe(stripe_current, include_indices);
    if (!batch_result.ok())
        throwFromArrowStatus(batch_result.status(), ErrorCodes::CANNOT_READ_ALL_DATA, "Failed to create batch reader");

    auto batch = batch_result.ValueOrDie();
    if (!batch)
        return {};

    /// Validate validity bitmaps before building the table: Table::FromRecordBatches computes
    /// each column's null_count, and Arrow derives an unknown FieldNode null_count by scanning
    /// the bitmap over the declared length, which reads out of bounds on a truncated bitmap.
    ArrowColumnToCHColumn::checkRecordBatchValidityBitmaps(*batch);

    auto table_result = arrow::Table::FromRecordBatches({batch});
    if (!table_result.ok())
        throwFromArrowStatus(table_result.status(), ErrorCodes::CANNOT_READ_ALL_DATA, "Error while reading batch of ORC data");

    /// We should extract the number of rows directly from the stripe, because in case when
    /// record batch contains 0 columns (for example if we requested only columns that
    /// are not presented in data) the number of rows in record batch will be 0.
    size_t num_rows = file_reader->GetRawORCReader()->getStripe(stripe_current)->getNumberOfRows();

    const auto & table = table_result.ValueOrDie();
    if (!table || !num_rows)
        return {};

    approx_bytes_read_for_chunk = file_reader->GetRawORCReader()->getStripe(stripe_current)->getDataLength();
    ++stripe_current;

    /// If defaults_for_omitted_fields is true, calculate the default values from default expression for omitted fields.
    /// Otherwise fill the missing columns with zero values of its type.
    BlockMissingValues * block_missing_values_ptr = format_settings.defaults_for_omitted_fields ? &block_missing_values : nullptr;
    std::shared_ptr<const arrow::KeyValueMetadata> metadata;
    if (auto status = file_reader->ReadMetadata(); status.ok())
        metadata = status.ValueOrDie();
    else
        throwFromArrowStatus(status.status(), ErrorCodes::BAD_ARGUMENTS, "Unexpected error while reading ORC metadata");

    return arrow_column_to_ch_column->arrowTableToCHChunk(table, num_rows, metadata, block_missing_values_ptr);
}

void ORCBlockInputFormat::resetParser()
{
    IInputFormat::resetParser();

    file_reader.reset();
    include_indices.clear();
    block_missing_values.clear();
}

const BlockMissingValues * ORCBlockInputFormat::getMissingValues() const
{
    return &block_missing_values;
}


static void getFileReaderAndSchema(
    ReadBuffer & in,
    std::unique_ptr<arrow::adapters::orc::ORCFileReader> & file_reader,
    std::shared_ptr<arrow::Schema> & schema,
    const FormatSettings & format_settings,
    std::atomic<int> & is_stopped)
{
    auto arrow_file = asArrowFile(in, format_settings, is_stopped, "ORC", ORC_MAGIC_BYTES);
    if (is_stopped)
        return;

    auto result = arrow::adapters::orc::ORCFileReader::Open(arrow_file, ArrowMemoryPool::instance());
    if (!result.ok())
        throwFromArrowStatus(result.status(), ErrorCodes::BAD_ARGUMENTS, "Failed to open ORC file");
    file_reader = std::move(result).ValueOrDie();

    auto read_schema_result = file_reader->ReadSchema();
    if (!read_schema_result.ok())
        throwFromArrowStatus(read_schema_result.status(), ErrorCodes::BAD_ARGUMENTS, "Failed to read ORC schema");
    schema = std::move(read_schema_result).ValueOrDie();
}

void ORCBlockInputFormat::prepareReader()
{
    std::shared_ptr<arrow::Schema> schema;
    getFileReaderAndSchema(*in, file_reader, schema, format_settings, is_stopped);
    if (is_stopped)
        return;

    stripe_total = static_cast<int>(file_reader->NumberOfStripes());
    stripe_current = 0;

    arrow_column_to_ch_column = std::make_unique<ArrowColumnToCHColumn>(
        getPort().getHeader(),
        "ORC",
        format_settings,
        std::nullopt,
        std::nullopt,
        format_settings.orc.allow_missing_columns,
        format_settings.null_as_default,
        format_settings.date_time_overflow_behavior,
        format_settings.parquet.allow_geoparquet_parser,
        format_settings.orc.case_insensitive_column_matching);

    const bool ignore_case = format_settings.orc.case_insensitive_column_matching;
    std::unordered_set<String> nested_table_names = Nested::getAllTableNames(getPort().getHeader(), ignore_case);
    for (int i = 0; i < schema->num_fields(); ++i)
    {
        const auto & name = schema->field(i)->name();
        if (getPort().getHeader().has(name, ignore_case) || nested_table_names.contains(ignore_case ? boost::to_lower_copy(name) : name))
            include_indices.push_back(i);
    }
}

ORCSchemaReader::ORCSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_)
    : ISchemaReader(in_), format_settings(format_settings_)
{
}

void ORCSchemaReader::initializeIfNeeded()
{
    if (file_reader)
        return;

    std::atomic<int> is_stopped = 0;
    getFileReaderAndSchema(in, file_reader, schema, format_settings, is_stopped);

    if (auto status = file_reader->ReadMetadata(); status.ok())
        metadata = status.ValueUnsafe();
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Error while reading incorrect metadata of ORC {}", status.status().message());
}

NamesAndTypesList ORCSchemaReader::readSchema()
{
    initializeIfNeeded();

    auto header = ArrowColumnToCHColumn::arrowSchemaToCHHeader(
        *schema,
        metadata,
        "ORC",
        format_settings,
        format_settings.orc.skip_columns_with_unsupported_types_in_schema_inference,
        format_settings.schema_inference_make_columns_nullable != 0,
        false,
        format_settings.parquet.allow_geoparquet_parser);
    if (format_settings.schema_inference_make_columns_nullable == 1)
        return getNamesAndRecursivelyNullableTypes(header, format_settings);
    return header.getNamesAndTypesList();
}

std::optional<size_t> ORCSchemaReader::readNumberOrRows()
{
    initializeIfNeeded();
    return file_reader->NumberOfRows();
}

void registerInputFormatORC(FormatFactory & factory);
void registerInputFormatORC(FormatFactory & factory)
{
    factory.registerRandomAccessInputFormat(
        "ORC",
        [](ReadBuffer & buf,
           const Block & sample,
           const FormatSettings & settings,
           const ReadSettings & read_settings,
           bool is_remote_fs,
           FormatParserSharedResourcesPtr,
           FormatFilterInfoPtr format_filter_info)
        {
            InputFormatPtr res;
            if (settings.orc.use_fast_decoder)
            {
                const bool has_file_size = isBufferWithFileSize(buf);
                auto * seekable_in = dynamic_cast<SeekableReadBuffer *>(&buf);
                const bool use_prefetch = is_remote_fs && read_settings.remote_fs_settings.prefetch && has_file_size && seekable_in
                    && seekable_in->checkIfActuallySeekable() && seekable_in->supportsReadAt() && settings.seekable_read;
                const size_t min_bytes_for_seek = use_prefetch ? read_settings.remote_fs_settings.min_bytes_for_seek : 0;
                res = std::make_shared<NativeORCBlockInputFormat>(
                    buf, std::make_shared<const Block>(sample), settings, use_prefetch, min_bytes_for_seek, format_filter_info);
            }
            else
                res = std::make_shared<ORCBlockInputFormat>(buf, std::make_shared<const Block>(sample), settings);

            return res;
        });
    factory.markFormatSupportsSubsetOfColumns("ORC");

    factory.setDocumentation("ORC", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

[Apache ORC](https://orc.apache.org/) is a columnar storage format widely used in the [Hadoop](https://hadoop.apache.org/) ecosystem.

## Data types matching {#data-types-matching-orc}

The table below compares supported ORC data types and their corresponding ClickHouse [data types](/sql-reference/data-types/index.md) in `INSERT` and `SELECT` queries.

| ORC data type (`INSERT`)              | ClickHouse data type                                                                                              | ORC data type (`SELECT`) |
|---------------------------------------|-------------------------------------------------------------------------------------------------------------------|--------------------------|
| `Boolean`                             | [UInt8](/sql-reference/data-types/int-uint.md)                                                            | `Boolean`                |
| `Tinyint`                             | [Int8/UInt8](/sql-reference/data-types/int-uint.md)/[Enum8](/sql-reference/data-types/enum.md)    | `Tinyint`                |
| `Smallint`                            | [Int16/UInt16](/sql-reference/data-types/int-uint.md)/[Enum16](/sql-reference/data-types/enum.md) | `Smallint`               |
| `Int`                                 | [Int32/UInt32](/sql-reference/data-types/int-uint.md)                                                     | `Int`                    |
| `Bigint`                              | [Int64/UInt32](/sql-reference/data-types/int-uint.md)                                                     | `Bigint`                 |
| `Float`                               | [Float32](/sql-reference/data-types/float.md)                                                             | `Float`                  |
| `Double`                              | [Float64](/sql-reference/data-types/float.md)                                                             | `Double`                 |
| `Decimal`                             | [Decimal](/sql-reference/data-types/decimal.md)                                                           | `Decimal`                |
| `Date`                                | [Date32](/sql-reference/data-types/date32.md)                                                             | `Date`                   |
| `Timestamp`                           | [DateTime64](/sql-reference/data-types/datetime64.md)                                                     | `Timestamp`              |
| `String`, `Char`, `Varchar`, `Binary` | [String](/sql-reference/data-types/string.md)                                                             | `Binary`                 |
| `List`                                | [Array](/sql-reference/data-types/array.md)                                                               | `List`                   |
| `Struct`                              | [Tuple](/sql-reference/data-types/tuple.md)                                                               | `Struct`                 |
| `Map`                                 | [Map](/sql-reference/data-types/map.md)                                                                   | `Map`                    |
| `Int`                                 | [IPv4](/sql-reference/data-types/int-uint.md)                                                             | `Int`                    |
| `Binary`                              | [IPv6](/sql-reference/data-types/ipv6.md)                                                                 | `Binary`                 |
| `Binary`                              | [Int128/UInt128/Int256/UInt256](/sql-reference/data-types/int-uint.md)                                    | `Binary`                 |
| `Binary`                              | [Decimal256](/sql-reference/data-types/decimal.md)                                                        | `Binary`                 |

- Other types are not supported.
- Arrays can be nested and can have a value of the `Nullable` type as an argument. `Tuple` and `Map` types also can be nested.
- The data types of ClickHouse table columns do not have to match the corresponding ORC data fields. When inserting data, ClickHouse interprets data types according to the table above and then [casts](/sql-reference/functions/type-conversion-functions#CAST) the data to the data type set for the ClickHouse table column.

## Example usage {#example-usage}

### Inserting data {#inserting-data}

Using an ORC file with the following data, named as `football.orc`:

```text
    ┌───────date─┬─season─┬─home_team─────────────┬─away_team───────────┬─home_team_goals─┬─away_team_goals─┐
 1. │ 2022-04-30 │   2021 │ Sutton United         │ Bradford City       │               1 │               4 │
 2. │ 2022-04-30 │   2021 │ Swindon Town          │ Barrow              │               2 │               1 │
 3. │ 2022-04-30 │   2021 │ Tranmere Rovers       │ Oldham Athletic     │               2 │               0 │
 4. │ 2022-05-02 │   2021 │ Port Vale             │ Newport County      │               1 │               2 │
 5. │ 2022-05-02 │   2021 │ Salford City          │ Mansfield Town      │               2 │               2 │
 6. │ 2022-05-07 │   2021 │ Barrow                │ Northampton Town    │               1 │               3 │
 7. │ 2022-05-07 │   2021 │ Bradford City         │ Carlisle United     │               2 │               0 │
 8. │ 2022-05-07 │   2021 │ Bristol Rovers        │ Scunthorpe United   │               7 │               0 │
 9. │ 2022-05-07 │   2021 │ Exeter City           │ Port Vale           │               0 │               1 │
10. │ 2022-05-07 │   2021 │ Harrogate Town A.F.C. │ Sutton United       │               0 │               2 │
11. │ 2022-05-07 │   2021 │ Hartlepool United     │ Colchester United   │               0 │               2 │
12. │ 2022-05-07 │   2021 │ Leyton Orient         │ Tranmere Rovers     │               0 │               1 │
13. │ 2022-05-07 │   2021 │ Mansfield Town        │ Forest Green Rovers │               2 │               2 │
14. │ 2022-05-07 │   2021 │ Newport County        │ Rochdale            │               0 │               2 │
15. │ 2022-05-07 │   2021 │ Oldham Athletic       │ Crawley Town        │               3 │               3 │
16. │ 2022-05-07 │   2021 │ Stevenage Borough     │ Salford City        │               4 │               2 │
17. │ 2022-05-07 │   2021 │ Walsall               │ Swindon Town        │               0 │               3 │
    └────────────┴────────┴───────────────────────┴─────────────────────┴─────────────────┴─────────────────┘
```

Insert the data:

```sql
INSERT INTO football FROM INFILE 'football.orc' FORMAT ORC;
```

### Reading data {#reading-data}

Read data using the `ORC` format:

```sql
SELECT *
FROM football
INTO OUTFILE 'football.orc'
FORMAT ORC
```

:::tip
ORC is a binary format that does not display in a human-readable form on the terminal. Use the `INTO OUTFILE` to output ORC files.
:::

## Format settings {#format-settings}

| Setting                                                                                                                                                                                                      | Description                                                                            | Default |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------|---------|
| [`output_format_arrow_string_as_string`](/operations/settings/settings-formats.md/#output_format_arrow_string_as_string)                                                                             | Use Arrow String type instead of Binary for String columns.                            | `false` |
| [`output_format_orc_compression_method`](/operations/settings/settings-formats.md/#output_format_orc_compression_method)                                                                             | Compression method used in output ORC format. Default value                            | `none`  |
| [`input_format_arrow_case_insensitive_column_matching`](/operations/settings/settings-formats.md/#input_format_arrow_case_insensitive_column_matching)                                               | Ignore case when matching Arrow columns with ClickHouse columns.                       | `false` |
| [`input_format_arrow_allow_missing_columns`](/operations/settings/settings-formats.md/#input_format_arrow_allow_missing_columns)                                                                     | Allow missing columns while reading Arrow data.                                        | `false` |
| [`input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference`](/operations/settings/settings-formats.md/#input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference) | Allow skipping columns with unsupported types while schema inference for Arrow format. | `false` |

To exchange data with Hadoop, you can use [HDFS table engine](/engines/table-engines/integrations/hdfs.md).
)DOCS_MD"});
}

void registerORCSchemaReader(FormatFactory & factory);
void registerORCSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "ORC",
        [](ReadBuffer & buf, const FormatSettings & settings)
        {
            SchemaReaderPtr res;
            if (settings.orc.use_fast_decoder)
                res = std::make_shared<NativeORCSchemaReader>(buf, settings);
            else
                res = std::make_shared<ORCSchemaReader>(buf, settings);

            return res;
        }
        );

    factory.registerAdditionalInfoForSchemaCacheGetter("ORC", [](const FormatSettings & settings)
    {
        return fmt::format(
            "schema_inference_make_columns_nullable={};schema_inference_allow_nullable_tuple_type={};use_fast_decoder={};dictionary_as_low_cardinality={}",
            settings.schema_inference_make_columns_nullable,
            settings.schema_inference_allow_nullable_tuple_type,
            settings.orc.use_fast_decoder,
            settings.orc.dictionary_as_low_cardinality);
    });
}

}
#else

namespace DB
{
    class FormatFactory;
    void registerInputFormatORC(FormatFactory &);
    void registerORCSchemaReader(FormatFactory &);
    void registerInputFormatORC(FormatFactory &)
    {
    }

    void registerORCSchemaReader(FormatFactory &)
    {
    }
}

#endif
