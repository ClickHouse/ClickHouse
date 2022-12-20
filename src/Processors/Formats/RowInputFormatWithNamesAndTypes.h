#pragma once

#include <Processors/Formats/RowInputFormatWithDiagnosticInfo.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>
#include <Formats/FormatFactory.h>

namespace DB
{

class FormatWithNamesAndTypesReader;

/// Base class for input formats with -WithNames and -WithNamesAndTypes suffixes.
/// It accepts 2 parameters in constructor - with_names and with_types and implements
/// input format depending on them:
///  - if with_names is true, it will expect that the first row of data contains column
///    names. If setting input_format_with_names_use_header is set to 1, columns mapping
///    will be performed.
///  - if with_types is true, it will expect that the second row of data contains column
///    types. If setting input_format_with_types_use_header is set to 1, types from input
///    will be compared types from header.
/// It's important that firstly this class reads/skips names and only
/// then reads/skips types. So you can this invariant.
class RowInputFormatWithNamesAndTypes : public RowInputFormatWithDiagnosticInfo
{
protected:
    /** is_binary - it is a binary format (e.g. don't search for BOM)
      * with_names - in the first line the header with column names
      * with_types - in the second line the header with column names
      */
    RowInputFormatWithNamesAndTypes(
        const Block & header_,
        ReadBuffer & in_,
        const Params & params_,
        bool is_binary_,
        bool with_names_,
        bool with_types_,
        const FormatSettings & format_settings_,
        std::unique_ptr<FormatWithNamesAndTypesReader> format_reader_);

    void resetParser() override;
    bool isGarbageAfterField(size_t index, ReadBuffer::Position pos) override;
    void setReadBuffer(ReadBuffer & in_) override;

    const FormatSettings format_settings;
    DataTypes data_types;
    bool end_of_stream = false;

private:
    bool readRow(MutableColumns & columns, RowReadExtension & ext) override;
    void readPrefix() override;

    bool parseRowAndPrintDiagnosticInfo(MutableColumns & columns, WriteBuffer & out) override;
    void tryDeserializeField(const DataTypePtr & type, IColumn & column, size_t file_column) override;

    bool is_binary;
    bool with_names;
    bool with_types;
    std::unique_ptr<FormatWithNamesAndTypesReader> format_reader;
    std::unordered_map<String, size_t> column_indexes_by_names;
};

/// Base class for parsing data in input formats with -WithNames and -WithNamesAndTypes suffixes.
/// Used for reading/skipping names/types/delimiters in specific format.
class FormatWithNamesAndTypesReader
{
public:
    explicit FormatWithNamesAndTypesReader(ReadBuffer & in_, const FormatSettings & format_settings_) : in(&in_), format_settings(format_settings_) {}

    /// Read single field from input. Return false if there was no real value and we inserted default value.
    virtual bool readField(IColumn & column, const DataTypePtr & type, const SerializationPtr & serialization, bool is_last_file_column, const String & column_name) = 0;

    /// Methods for parsing with diagnostic info.
    virtual void checkNullValueForNonNullable(DataTypePtr) {}
    virtual bool parseRowStartWithDiagnosticInfo(WriteBuffer &) { return true; }
    virtual bool parseFieldDelimiterWithDiagnosticInfo(WriteBuffer &) { return true; }
    virtual bool parseRowEndWithDiagnosticInfo(WriteBuffer &) { return true;}
    virtual bool parseRowBetweenDelimiterWithDiagnosticInfo(WriteBuffer &) { return true;}
    virtual bool tryParseSuffixWithDiagnosticInfo(WriteBuffer &) { return true; }
    virtual bool isGarbageAfterField(size_t, ReadBuffer::Position) { return false; }

    /// Read row with names and return the list of them.
    virtual std::vector<String> readNames() = 0;
    /// Read row with types and return the list of them.
    virtual std::vector<String> readTypes() = 0;

    /// Skip single field, it's used to skip unknown columns.
    virtual void skipField(size_t file_column) = 0;
    /// Skip the whole row with names.
    virtual void skipNames() = 0;
    /// Skip the whole row with types.
    virtual void skipTypes() = 0;

    /// Skip delimiters, if any.
    virtual void skipPrefixBeforeHeader() {}
    virtual void skipRowStartDelimiter() {}
    virtual void skipFieldDelimiter() {}
    virtual void skipRowEndDelimiter() {}
    virtual void skipRowBetweenDelimiter() {}

    /// Check suffix.
    virtual bool checkForSuffix() { return in->eof(); }

    const FormatSettings & getFormatSettings() const { return format_settings; }

    virtual void setReadBuffer(ReadBuffer & in_) { in = &in_; }

    virtual ~FormatWithNamesAndTypesReader() = default;

protected:
    ReadBuffer * in;
    const FormatSettings format_settings;
};

/// Base class for schema inference for formats with -WithNames and -WithNamesAndTypes suffixes.
/// For formats with -WithNamesAndTypes suffix the schema will be determined by first two rows.
/// For formats with -WithNames suffix the names of columns will be determined by the first row
/// and types of columns by the rows with data.
/// For formats without suffixes default column names will be used
/// and types will be determined by the rows with data.
class FormatWithNamesAndTypesSchemaReader : public IRowSchemaReader
{
public:
    FormatWithNamesAndTypesSchemaReader(
        ReadBuffer & in,
        const FormatSettings & format_settings_,
        bool with_names_,
        bool with_types_,
        FormatWithNamesAndTypesReader * format_reader_,
        DataTypePtr default_type_ = nullptr);

    NamesAndTypesList readSchema() override;

protected:
    virtual DataTypes readRowAndGetDataTypes() override = 0;

    bool with_names;
    bool with_types;

private:
    FormatWithNamesAndTypesReader * format_reader;
};

}

