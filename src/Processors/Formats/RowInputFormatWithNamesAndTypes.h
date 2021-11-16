#pragma once

#include <Processors/Formats/RowInputFormatWithDiagnosticInfo.h>
#include <Formats/FormatSettings.h>
#include <Formats/FormatFactory.h>

namespace DB
{

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
public:
    /** with_names - in the first line the header with column names
      * with_types - in the second line the header with column names
      */
    RowInputFormatWithNamesAndTypes(
        const Block & header_,
        ReadBuffer & in_,
        const Params & params_,
        bool with_names_, bool with_types_, const FormatSettings & format_settings_);

    bool readRow(MutableColumns & columns, RowReadExtension & ext) override;
    void readPrefix() override;
    void resetParser() override;

protected:
    /// Read single field from input. Return false if there was no real value and we inserted default value.
    virtual bool readField(IColumn & column, const DataTypePtr & type, const SerializationPtr & serialization, bool is_last_file_column, const String & column_name) = 0;

    /// Skip single field, it's used to skip unknown columns.
    virtual void skipField(size_t file_column) = 0;
    /// Skip the whole row with names.
    virtual void skipNames() = 0;
    /// Skip the whole row with types.
    virtual void skipTypes() = 0;

    /// Skip delimiters, if any.
    virtual void skipRowStartDelimiter() {}
    virtual void skipFieldDelimiter() {}
    virtual void skipRowEndDelimiter() {}


    /// Methods for parsing with diagnostic info.
    virtual void checkNullValueForNonNullable(DataTypePtr) {}
    virtual bool parseRowStartWithDiagnosticInfo(WriteBuffer &) { return true; }
    virtual bool parseFieldDelimiterWithDiagnosticInfo(WriteBuffer &) { return true; }
    virtual bool parseRowEndWithDiagnosticInfo(WriteBuffer &) { return true;}
    bool isGarbageAfterField(size_t, ReadBuffer::Position) override {return false; }

    /// Read row with names and return the list of them.
    virtual std::vector<String> readNames() = 0;
    /// Read row with types and return the list of them.
    virtual std::vector<String> readTypes() = 0;

    const FormatSettings format_settings;
    DataTypes data_types;

private:
    bool parseRowAndPrintDiagnosticInfo(MutableColumns & columns, WriteBuffer & out) override;
    void tryDeserializeField(const DataTypePtr & type, IColumn & column, size_t file_column) override;

    void setupAllColumnsByTableSchema();
    void addInputColumn(const String & column_name, std::vector<bool> & read_columns);
    void insertDefaultsForNotSeenColumns(MutableColumns & columns, RowReadExtension & ext);

    bool with_names;
    bool with_types;
    std::unordered_map<String, size_t> column_indexes_by_names;
};

void registerFileSegmentationEngineForFormatWithNamesAndTypes(
    FormatFactory & factory, const String & base_format_name, FormatFactory::FileSegmentationEngine segmentation_engine);

}
