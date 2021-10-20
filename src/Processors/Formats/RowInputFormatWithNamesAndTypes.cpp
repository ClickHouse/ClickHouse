#include <Processors/Formats/RowInputFormatWithNamesAndTypes.h>
#include <DataTypes/DataTypeNothing.h>
#include <IO/ReadHelpers.h>
#include <IO/Operators.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

RowInputFormatWithNamesAndTypes::RowInputFormatWithNamesAndTypes(
    const Block & header_,
    ReadBuffer & in_,
    const Params & params_,
    bool with_names_,
    bool with_types_,
    const FormatSettings & format_settings_)
    : RowInputFormatWithDiagnosticInfo(header_, in_, params_), format_settings(format_settings_), with_names(with_names_), with_types(with_types_)
{
    const auto & sample = getPort().getHeader();
    size_t num_columns = sample.columns();

    data_types.resize(num_columns);
    column_indexes_by_names.reserve(num_columns);

    for (size_t i = 0; i < num_columns; ++i)
    {
        const auto & column_info = sample.getByPosition(i);

        data_types[i] = column_info.type;
        column_indexes_by_names.emplace(column_info.name, i);
    }
}

void RowInputFormatWithNamesAndTypes::setupAllColumnsByTableSchema()
{
    const auto & header = getPort().getHeader();
    column_mapping->column_indexes_for_input_fields.resize(header.columns());
    column_mapping->names_of_columns = header.getNames();

    for (size_t i = 0; i < column_mapping->column_indexes_for_input_fields.size(); ++i)
        column_mapping->column_indexes_for_input_fields[i] = i;
}

void RowInputFormatWithNamesAndTypes::addInputColumn(const String & column_name, std::vector<bool> & read_columns)
{
    column_mapping->names_of_columns.push_back(column_name);

    const auto column_it = column_indexes_by_names.find(column_name);
    if (column_it == column_indexes_by_names.end())
    {
        if (format_settings.skip_unknown_fields)
        {
            column_mapping->column_indexes_for_input_fields.push_back(std::nullopt);
            return;
        }

        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Unknown field found in {} header: '{}' at position {}\nSet the 'input_format_skip_unknown_fields' parameter explicitly to ignore and proceed",
            getName(), column_name, column_mapping->column_indexes_for_input_fields.size());
    }

    const auto column_index = column_it->second;

    if (read_columns[column_index])
        throw Exception("Duplicate field found while parsing TSV header: " + column_name, ErrorCodes::INCORRECT_DATA);

    read_columns[column_index] = true;
    column_mapping->column_indexes_for_input_fields.emplace_back(column_index);
}

void RowInputFormatWithNamesAndTypes::readPrefix()
{
    if (with_names || with_types || data_types.at(0)->textCanContainOnlyValidUTF8())
    {
        /// We assume that column name or type cannot contain BOM, so, if format has header,
        /// then BOM at beginning of stream cannot be confused with name or type of field, and it is safe to skip it.
        skipBOMIfExists(*in);
    }

    /// This is a bit of abstraction leakage, but we need it in parallel parsing:
    /// we check if this InputFormat is working with the "real" beginning of the data.
    if (with_names && getCurrentUnitNumber() == 0)
    {
        if (format_settings.with_names_use_header)
        {
            std::vector<bool> read_columns(data_types.size(), false);
            auto column_names = readNames();
            for (const auto & name : column_names)
                addInputColumn(name, read_columns);

            for (size_t i = 0; i != read_columns.size(); ++i)
            {
                if (!read_columns[i])
                    column_mapping->not_presented_columns.push_back(i);
            }
        }
        else
        {
            setupAllColumnsByTableSchema();
            skipNames();
        }
    }
    else if (!column_mapping->is_set)
        setupAllColumnsByTableSchema();

    if (with_types && getCurrentUnitNumber() == 0)
    {
        if (format_settings.with_types_use_header)
        {
            auto types = readTypes();
            if (types.size() != column_mapping->column_indexes_for_input_fields.size())
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "The number of data types differs from the number of column names in input data");

            /// Check that types from input matches types from header.
            for (size_t i = 0; i < types.size(); ++i)
            {
                if (column_mapping->column_indexes_for_input_fields[i] &&
                    data_types[*column_mapping->column_indexes_for_input_fields[i]]->getName() != types[i])
                {
                    throw Exception(
                        ErrorCodes::INCORRECT_DATA,
                        "Type of '{}' must be {}, not {}",
                        getPort().getHeader().getByPosition(*column_mapping->column_indexes_for_input_fields[i]).name,
                        data_types[*column_mapping->column_indexes_for_input_fields[i]]->getName(), types[i]);
                }
            }
        }
        else
            skipTypes();
    }
}

void RowInputFormatWithNamesAndTypes::insertDefaultsForNotSeenColumns(MutableColumns & columns, RowReadExtension & ext)
{
    for (auto index : column_mapping->not_presented_columns)
    {
        columns[index]->insertDefault();
        ext.read_columns[index] = false;
    }
}

bool RowInputFormatWithNamesAndTypes::readRow(MutableColumns & columns, RowReadExtension & ext)
{
    if (in->eof())
        return false;

    updateDiagnosticInfo();
    skipRowStartDelimiter();

    ext.read_columns.resize(data_types.size());
    for (size_t file_column = 0; file_column < column_mapping->column_indexes_for_input_fields.size(); ++file_column)
    {
        const auto & column_index = column_mapping->column_indexes_for_input_fields[file_column];
        const bool is_last_file_column = file_column + 1 == column_mapping->column_indexes_for_input_fields.size();
        if (column_index)
            ext.read_columns[*column_index] = readField(
                *columns[*column_index],
                data_types[*column_index],
                serializations[*column_index],
                is_last_file_column,
                column_mapping->names_of_columns[file_column]);
        else
            skipField(file_column);

        if (!is_last_file_column)
            skipFieldDelimiter();
    }

    skipRowEndDelimiter();

    insertDefaultsForNotSeenColumns(columns, ext);

    /// If defaults_for_omitted_fields is set to 0, we should leave already inserted defaults.
    if (!format_settings.defaults_for_omitted_fields)
        ext.read_columns.assign(ext.read_columns.size(), true);

    return true;
}

void RowInputFormatWithNamesAndTypes::resetParser()
{
    RowInputFormatWithDiagnosticInfo::resetParser();
    column_mapping->column_indexes_for_input_fields.clear();
    column_mapping->not_presented_columns.clear();
    column_mapping->names_of_columns.clear();
}

void RowInputFormatWithNamesAndTypes::tryDeserializeField(const DataTypePtr & type, IColumn & column, size_t file_column)
{
    const auto & index = column_mapping->column_indexes_for_input_fields[file_column];
    if (index)
    {
        checkNullValueForNonNullable(type);
        const bool is_last_file_column = file_column + 1 == column_mapping->column_indexes_for_input_fields.size();
        readField(column, type, serializations[*index], is_last_file_column, column_mapping->names_of_columns[file_column]);
    }
    else
    {
        skipField(file_column);
    }
}

bool RowInputFormatWithNamesAndTypes::parseRowAndPrintDiagnosticInfo(MutableColumns & columns, WriteBuffer & out)
{
    if (in->eof())
    {
        out << "<End of stream>\n";
        return false;
    }

    if (!parseRowStartWithDiagnosticInfo(out))
        return false;

    for (size_t file_column = 0; file_column < column_mapping->column_indexes_for_input_fields.size(); ++file_column)
    {
        if (column_mapping->column_indexes_for_input_fields[file_column].has_value())
        {
            const auto & header = getPort().getHeader();
            size_t col_idx = column_mapping->column_indexes_for_input_fields[file_column].value();
            if (!deserializeFieldAndPrintDiagnosticInfo(header.getByPosition(col_idx).name, data_types[col_idx], *columns[col_idx], out, file_column))
                return false;
        }
        else
        {
            static const String skipped_column_str = "<SKIPPED COLUMN>";
            static const DataTypePtr skipped_column_type = std::make_shared<DataTypeNothing>();
            static const MutableColumnPtr skipped_column = skipped_column_type->createColumn();
            if (!deserializeFieldAndPrintDiagnosticInfo(skipped_column_str, skipped_column_type, *skipped_column, out, file_column))
                return false;
        }

        /// Delimiters
        if (file_column + 1 != column_mapping->column_indexes_for_input_fields.size())
        {
            if (!parseFieldDelimiterWithDiagnosticInfo(out))
                return false;
        }
    }

    return parseRowEndWithDiagnosticInfo(out);
}


void registerFileSegmentationEngineForFormatWithNamesAndTypes(
    FormatFactory & factory, const String & base_format_name, FormatFactory::FileSegmentationEngine segmentation_engine)
{
    factory.registerFileSegmentationEngine(base_format_name, segmentation_engine);
    factory.registerFileSegmentationEngine(base_format_name + "WithNames", segmentation_engine);
    factory.registerFileSegmentationEngine(base_format_name + "WithNamesAndTypes", segmentation_engine);
}


}
