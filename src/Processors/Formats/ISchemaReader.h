#pragma once

#include <Core/NamesAndTypes.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBuffer.h>

namespace DB
{

/// Base class for schema inference for the data in some specific format.
/// It reads some data from read buffer and try to determine the schema
/// from read data.
class ISchemaReader
{
public:
    explicit ISchemaReader(ReadBuffer & in_) : in(in_) {}

    virtual NamesAndTypesList readSchema() = 0;

    virtual ~ISchemaReader() = default;

protected:
    ReadBuffer & in;
};

/// Base class for schema inference for formats that read data row by row.
/// It reads data row by row (up to max_rows_to_read), determines types of columns
/// for each row and compare them with types from the previous rows. If some column
/// contains values with different types in different rows, the default type will be
/// used for this column or the exception will be thrown (if default type is not set).
class IRowSchemaReader : public ISchemaReader
{
public:
    IRowSchemaReader(ReadBuffer & in_, size_t max_rows_to_read_, DataTypePtr default_type_ = nullptr);
    NamesAndTypesList readSchema() override;

protected:
    /// Read one row and determine types of columns in it.
    /// Return types in the same order in which the values were in the row.
    /// If it's impossible to determine the type for some column, return nullptr for it.
    /// Return empty list if can't read more data.
    virtual DataTypes readRowAndGetDataTypes() = 0;

    void setColumnNames(const std::vector<String> & names) { column_names = names; }

private:
    size_t max_rows_to_read;
    DataTypePtr default_type;
    std::vector<String> column_names;
};

/// Base class for schema inference for formats that read data row by row and each
/// row contains column names and values (ex: JSONEachRow, TSKV).
/// Differ from IRowSchemaReader in that after reading a row we get
/// a map {column_name : type} and some columns may be missed in a single row
/// (in this case we will use types from the previous rows for missed columns).
class IRowWithNamesSchemaReader : public ISchemaReader
{
public:
    IRowWithNamesSchemaReader(ReadBuffer & in_, size_t max_rows_to_read_, DataTypePtr default_type_ = nullptr);
    NamesAndTypesList readSchema() override;

protected:
    /// Read one row and determine types of columns in it.
    /// Return map {column_name : type}.
    /// If it's impossible to determine the type for some column, return nullptr for it.
    /// Return empty map is can't read more data.
    virtual std::unordered_map<String, DataTypePtr> readRowAndGetNamesAndDataTypes() = 0;

private:
    size_t max_rows_to_read;
    DataTypePtr default_type;
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

}
