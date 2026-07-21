#pragma once

#include <Core/NamesAndTypes.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBuffer.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int INCORRECT_DATA;
}

/// Base class for schema inference for the data in some specific format.
/// It reads some data from read buffer and tries to determine the schema
/// from read data.
class ISchemaReader
{
public:
    explicit ISchemaReader(ReadBuffer & in_) : in(in_) {}

    virtual NamesAndTypesList readSchema() = 0;

    /// Some formats like Parquet contains number of rows in metadata
    /// and we can read it once during schema inference and reuse it later for fast count;
    virtual std::optional<size_t> readNumberOrRows() { return std::nullopt; }

    /// True if order of columns is important in format.
    /// Exceptions: JSON, TSKV.
    virtual bool hasStrictOrderOfColumns() const { return true; }

    /// True if the column types are declared in the data itself (e.g. the second header row of the
    /// -WithNamesAndTypes formats) and are validated by the parser against the destination types
    /// exactly, rather than inferred from the data values (and therefore widened). A caller that
    /// compares an inferred schema against an expected one can use this to know when an exact type
    /// comparison mirrors the parser and a loose (supertype-based) one would be wrong.
    virtual bool hasExactTypesFromData() const { return false; }

    /// True when the schema returned by `readSchema` describes the structure the parser actually reads
    /// and validates. It is false for the metadata-based `JSON*` formats (`JSON`, `JSONCompact`,
    /// `JSONColumnsWithMetadata`) when `input_format_json_validate_types_from_metadata` = 0: schema
    /// inference reads the types declared in the `meta` section, but the parser then ignores those
    /// types entirely and reads the data by value (and, for `JSONCompact`, positionally). The inferred
    /// schema therefore does not correspond to what is parsed, and a caller comparing an inferred schema
    /// against an expected one must not draw any conclusion from it.
    virtual bool schemaDescribesParsedData() const { return true; }

    /// True if the format legally accepts a number of columns that differs from the destination: missing
    /// trailing columns are filled with defaults and/or extra columns are skipped, so the number of columns
    /// present in the data is not by itself a reliable structure-mismatch signal. It is always true for the
    /// columnar `JSONCompactColumns` format and setting-gated for `CSV` / `TSV` / `CustomSeparated` /
    /// `JSONCompactEachRow` (their `*_allow_variable_number_of_columns` settings). It is also consulted by
    /// `IRowSchemaReader::readSchema` to decide whether rows with a varying number of values are allowed.
    virtual bool allowVariableNumberOfColumns() const { return false; }

    /// True if the parser of the format reads typed JSON value tokens (a bare number, `true` / `false`,
    /// an array, an object) and consults the `input_format_json_read_*_as_strings` settings to decide
    /// whether such a token may be read into a `String` column. It is false for the flat-text formats
    /// (`TSV`, `CSV`, `TSKV`, ...), which read every field verbatim into a `String` column regardless of
    /// those settings, and for the `-Strings` JSON variants, whose values are all strings rather than
    /// typed tokens (see `readsStringValuesAsWholeText` below). A caller that compares an inferred schema against an expected one can use this to
    /// know when an inferred non-`String` type going into a `String` destination follows the JSON
    /// settings and when it is unconditionally accepted.
    virtual bool readsTypedJSONValueTokens() const { return false; }

    /// True if every value in the format is a string whose content the parser re-parses with the
    /// whole-text deserializer of the destination type (the `-Strings` JSON variants,
    /// `JSONStringsEachRow` / `JSONCompactStringsEachRow` / ...). There the destination type sees the
    /// unquoted content of the string, so, for example, a quoted `"1"` is accepted into a `Bool`
    /// column, while the typed-token JSON formats reject any string token there and the flat-text
    /// formats hand the `Bool` deserializer the raw (still quoted) field. A caller that compares an
    /// inferred schema against an expected one can use this to know when an inferred `String` says
    /// nothing about the parsability of its content into the destination type.
    virtual bool readsStringValuesAsWholeText() const { return false; }

    /// True if the parser reads the raw representation of a field of any type verbatim into a `String`
    /// destination column, as the flat-text formats (`TSV`, `CSV`, ...) do. It is false for formats
    /// that store typed values and reject a non-string value for a `String` column (`BSONEachRow`,
    /// `MsgPack`). It is not consulted for the typed-token JSON formats, which govern this per token
    /// type via the `input_format_json_read_*_as_strings` settings (see `readsTypedJSONValueTokens`
    /// above). A caller that compares an inferred schema against an expected one can use this to know
    /// when an inferred non-`String` type going into a `String` destination is a genuine structure
    /// mismatch.
    virtual bool readsAnyValueIntoStringColumn() const { return true; }

    /// True when the parser maps the input's fields to destination columns by name rather than by
    /// position. Besides the inherently name-based formats (`JSONEachRow`, `TSKV`, `BSONEachRow`, ...,
    /// which also return `hasStrictOrderOfColumns() == false`), a `*WithNames*` format carries a names
    /// header and the parser maps its columns by name when configured to use that header
    /// (`input_format_with_names_use_header`) — even for a format that does not advertise
    /// `FormatFactory::checkIfFormatSupportsSubsetOfColumns` (e.g. `RowBinaryWithNamesAndTypes` and
    /// `RowBinaryWithNamesAndTypesAndDefaults`). A caller comparing an inferred schema against an
    /// expected one uses this to match columns by name (tolerating a reordered header) instead of
    /// positionally.
    virtual bool mapsColumnsByName() const { return false; }

    /// True when the parser accepts a bare numeric value into an `IPv4` destination column. Most formats
    /// require a (quoted) string for `IPv4` — the text / JSON deserializers reject a number — but the
    /// binary formats that store typed values read an integer straight into the `UInt32`-backed `IPv4`
    /// column (`BSONEachRow` via a BSON `Int32`, `MsgPack` via its `TypeIndex::IPv4` integer arm). A
    /// caller comparing an inferred schema against an expected one uses this to avoid flagging an
    /// inferred numeric type going into an `IPv4` column as a structure mismatch for these formats.
    /// (`UUID` and `IPv6` still require binary data of the exact size in every format, so they stay a
    /// mismatch regardless of this capability.)
    virtual bool readsNumericValueIntoIPv4Column() const { return false; }

    virtual bool needContext() const { return false; }
    virtual void setContext(const ContextPtr &) {}

    virtual void setMaxRowsAndBytesToRead(size_t, size_t) {}
    virtual size_t getNumRowsRead() const { return 0; }

    virtual void transformTypesIfNeeded(DataTypePtr & type, DataTypePtr & new_type);
    virtual void transformTypesFromDifferentFilesIfNeeded(DataTypePtr & type, DataTypePtr & new_type) { transformTypesIfNeeded(type, new_type); }

    virtual ~ISchemaReader() = default;

protected:
    ReadBuffer & in;
};

using CommonDataTypeChecker = std::function<DataTypePtr(const DataTypePtr &, const DataTypePtr &)>;

class IIRowSchemaReader : public ISchemaReader
{
public:
    IIRowSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_, DataTypePtr default_type_ = nullptr);

    bool needContext() const override { return !hints_str.empty(); }
    void setContext(const ContextPtr & context) override;

protected:
    void setMaxRowsAndBytesToRead(size_t max_rows, size_t max_bytes) override
    {
        max_rows_to_read = max_rows;
        max_bytes_to_read = max_bytes;
    }
    size_t getNumRowsRead() const override { return rows_read; }

    virtual void transformFinalTypeIfNeeded(DataTypePtr &) {}

    size_t max_rows_to_read;
    size_t max_bytes_to_read;
    size_t rows_read = 0;
    DataTypePtr default_type;
    String hints_str;
    FormatSettings format_settings;
    std::unordered_map<String, DataTypePtr> hints;
    String hints_parsing_error;
};

/// Base class for schema inference for formats that read data row by row.
/// It reads data row by row (up to max_rows_to_read), determines types of columns
/// for each row and compare them with types from the previous rows. If some column
/// contains values with different types in different rows, the default type
/// (from argument default_type_) will be used for this column or the exception
/// will be thrown (if default type is not set). If different columns have different
/// default types, you can provide them by default_types_ argument.
class IRowSchemaReader : public IIRowSchemaReader
{
public:
    IRowSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_);
    IRowSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_, DataTypePtr default_type_);
    IRowSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_, const DataTypes & default_types_);

    NamesAndTypesList readSchema() override;

protected:
    /// Read one row and determine types of columns in it.
    /// Return types in the same order in which the values were in the row.
    /// If it's impossible to determine the type for some column, return nullptr for it.
    /// Return std::nullopt if can't read more data.
    virtual std::optional<DataTypes> readRowAndGetDataTypes() = 0;

    void setColumnNames(const std::vector<String> & names) { column_names = names; }

    size_t field_index{};

private:
    DataTypePtr getDefaultType(size_t column) const;
    void initColumnNames(const String & column_names_str);

    DataTypes default_types;
    std::vector<String> column_names;
};

/// Base class for schema inference for formats that read data row by row and each
/// row contains column names and values (ex: JSONEachRow, TSKV).
/// Differ from IRowSchemaReader in that after reading a row we get
/// a map {column_name : type} and some columns may be missed in a single row
/// (in this case we will use types from the previous rows for missed columns).
class IRowWithNamesSchemaReader : public IIRowSchemaReader
{
public:
    IRowWithNamesSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_, DataTypePtr default_type_ = nullptr);
    NamesAndTypesList readSchema() override;
    bool hasStrictOrderOfColumns() const override { return false; }

protected:
    /// Read one row and determine types of columns in it.
    /// Return list with names and types.
    /// If it's impossible to determine the type for some column, return nullptr for it.
    /// Set eof = true if can't read more data.
    virtual NamesAndTypesList readRowAndGetNamesAndDataTypes(bool & eof) = 0;

    /// Get special static types that have the same name/type for each row.
    /// For example, in JSONObjectEachRow format we have static column with
    /// type String and name from a settings for object keys.
    virtual NamesAndTypesList getStaticNamesAndTypes() { return {}; }
};

/// Base class for schema inference for formats that don't need any data to
/// determine the schema: formats with constant schema (ex: JSONAsString, LineAsString)
/// and formats that use external format schema (ex: Protobuf, CapnProto).
class IExternalSchemaReader
{
public:
    virtual NamesAndTypesList readSchema() = 0;

    virtual ~IExternalSchemaReader() = default;
};

template <class SchemaReader>
void chooseResultColumnType(
    SchemaReader & schema_reader,
    DataTypePtr & type,
    DataTypePtr & new_type,
    const DataTypePtr & default_type,
    const String & column_name,
    size_t row,
    const String & hints_parsing_error = "")
{
    if (!type)
    {
        type = new_type;
        return;
    }

    if (!new_type || type->equals(*new_type))
        return;

    schema_reader.transformTypesIfNeeded(type, new_type);
    if (type->equals(*new_type))
        return;

    /// If the new type and the previous type for this column are different,
    /// we will use default type if we have it or throw an exception.
    if (default_type)
        type = default_type;
    else
    {
        if (hints_parsing_error.empty())
            throw Exception(
                ErrorCodes::TYPE_MISMATCH,
                "Automatically defined type {} for column '{}' in row {} differs from type defined by previous rows: {}. "
                "You can specify the type for this column using setting schema_inference_hints",
                new_type->getName(),
                column_name,
                row,
                type->getName());
        throw Exception(
            ErrorCodes::TYPE_MISMATCH,
            "Automatically defined type {} for column '{}' in row {} differs from type defined by previous rows: {}. "
            "Column types from setting schema_inference_hints couldn't be parsed because of error: {}",
            new_type->getName(),
            column_name,
            row,
            type->getName(),
            hints_parsing_error);
    }
}

template <class SchemaReader>
void chooseResultColumnTypes(
    SchemaReader & schema_reader,
    DataTypes & types,
    DataTypes & new_types,
    const DataTypePtr & default_type,
    const std::vector<String> & column_names,
    size_t row)
{
    if (types.size() != new_types.size())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Rows have different amount of values");

    if (types.size() != column_names.size())
        throw Exception(ErrorCodes::INCORRECT_DATA, "The number of column names {} differs from the number of types {}", column_names.size(), types.size());

    for (size_t i = 0; i != types.size(); ++i)
        chooseResultColumnType(schema_reader, types[i], new_types[i], default_type, column_names[i], row);
}

void checkFinalInferredType(
    DataTypePtr & type,
    const String & name,
    const FormatSettings & settings,
    const DataTypePtr & default_type,
    size_t rows_read,
    const String & hints_parsing_error);

Strings splitColumnNames(const String & column_names_str);

}
