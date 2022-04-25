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

    /// True if order of columns is important in format.
    /// Exceptions: JSON, TSKV.
    virtual bool hasStrictOrderOfColumns() const { return true; }

    virtual ~ISchemaReader() = default;

protected:
    ReadBuffer & in;
};

using CommonDataTypeChecker = std::function<DataTypePtr(const DataTypePtr &, const DataTypePtr &)>;

/// Base class for schema inference for formats that read data row by row.
/// It reads data row by row (up to max_rows_to_read), determines types of columns
/// for each row and compare them with types from the previous rows. If some column
/// contains values with different types in different rows, the default type
/// (from argument default_type_) will be used for this column or the exception
/// will be thrown (if default type is not set). If different columns have different
/// default types, you can provide them by default_types_ argument.
class IRowSchemaReader : public ISchemaReader
{
public:
    IRowSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings);
    IRowSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings, DataTypePtr default_type_);
    IRowSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings, const DataTypes & default_types_);

    NamesAndTypesList readSchema() override;

    void setCommonTypeChecker(CommonDataTypeChecker checker) { common_type_checker = checker; }

protected:
    /// Read one row and determine types of columns in it.
    /// Return types in the same order in which the values were in the row.
    /// If it's impossible to determine the type for some column, return nullptr for it.
    /// Return empty list if can't read more data.
    virtual DataTypes readRowAndGetDataTypes() = 0;

    void setColumnNames(const std::vector<String> & names) { column_names = names; }

private:

    DataTypePtr getDefaultType(size_t column) const;
    size_t max_rows_to_read;
    DataTypePtr default_type;
    DataTypes default_types;
    CommonDataTypeChecker common_type_checker;
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
    bool hasStrictOrderOfColumns() const override { return false; }

    void setCommonTypeChecker(CommonDataTypeChecker checker) { common_type_checker = checker; }

protected:
    /// Read one row and determine types of columns in it.
    /// Return list with names and types.
    /// If it's impossible to determine the type for some column, return nullptr for it.
    /// Set eof = true if can't read more data.
    virtual NamesAndTypesList readRowAndGetNamesAndDataTypes(bool & eof) = 0;

private:
    size_t max_rows_to_read;
    DataTypePtr default_type;
    CommonDataTypeChecker common_type_checker;
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
