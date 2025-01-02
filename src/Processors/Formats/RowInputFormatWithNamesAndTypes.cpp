#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <Formats/EscapingRuleUtils.h>
#include <IO/Operators.h>
#include <IO/PeekableReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Processors/Formats/Impl/BinaryRowInputFormat.h>
#include <Processors/Formats/Impl/CSVRowInputFormat.h>
#include <Processors/Formats/Impl/CustomSeparatedRowInputFormat.h>
#include <Processors/Formats/Impl/HiveTextRowInputFormat.h>
#include <Processors/Formats/Impl/JSONCompactRowInputFormat.h>
#include <Processors/Formats/Impl/TabSeparatedRowInputFormat.h>
#include <Processors/Formats/RowInputFormatWithNamesAndTypes.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
    extern const int TYPE_MISMATCH;
}

namespace
{
    bool checkIfAllValuesAreTypeNames(const std::vector<String> & names)
    {
        for (const auto & name : names)
        {
            if (!DataTypeFactory::instance().tryGet(name))
                return false;
        }
        return true;
    }

    bool isSubsetOf(const std::unordered_set<std::string_view> & subset, const std::unordered_set<std::string_view> & set)
    {
        for (const auto & element : subset)
        {
            if (!set.contains(element))
                return false;
        }
        return true;
    }
}

template <typename FormatReaderImpl>
RowInputFormatWithNamesAndTypes<FormatReaderImpl>::RowInputFormatWithNamesAndTypes(
    const Block & header_,
    ReadBuffer & in_,
    const Params & params_,
    bool is_binary_,
    bool with_names_,
    bool with_types_,
    const FormatSettings & format_settings_,
    std::unique_ptr<FormatReaderImpl> format_reader_,
    bool try_detect_header_)
    : RowInputFormatWithDiagnosticInfo(header_, in_, params_)
    , format_settings(format_settings_)
    , data_types(header_.getDataTypes())
    , is_binary(is_binary_)
    , with_names(with_names_)
    , with_types(with_types_)
    , try_detect_header(try_detect_header_)
    , format_reader(std::move(format_reader_))
{
    column_indexes_by_names = getPort().getHeader().getNamesToIndexesMap();
}

template <typename FormatReaderImpl>
void RowInputFormatWithNamesAndTypes<FormatReaderImpl>::readPrefix()
{
    /// Search and remove BOM only in textual formats (CSV, TSV etc), not in binary ones (RowBinary*).
    /// Also, we assume that column name or type cannot contain BOM, so, if format has header,
    /// then BOM at beginning of stream cannot be confused with name or type of field, and it is safe to skip it.
    if (!is_binary && (with_names || with_types || data_types.at(0)->textCanContainOnlyValidUTF8()))
    {
        skipBOMIfExists(*in);
    }

    /// Skip prefix before names and types.
    format_reader->skipPrefixBeforeHeader();

    std::vector<String> column_names;
    std::vector<String> type_names;
    if (with_names)
        column_names = format_reader->readNames();

    if (with_types)
    {
        /// Skip delimiter between names and types.
        format_reader->skipRowBetweenDelimiter();
        type_names = format_reader->readTypes();
    }

    if (!with_names && !with_types && try_detect_header)
        tryDetectHeader(column_names, type_names);

    if (!column_names.empty())
    {
        if (format_settings.with_names_use_header)
            column_mapping->addColumns(column_names, column_indexes_by_names, format_settings);
        else
            column_mapping->setupByHeader(getPort().getHeader());
    }
    else if (!column_mapping->is_set)
        column_mapping->setupByHeader(getPort().getHeader());

    if (!type_names.empty() && format_settings.with_types_use_header)
    {
        if (type_names.size() != column_mapping->column_indexes_for_input_fields.size())
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "The number of data types differs from the number of column names in input data");

        /// Check that types from input matches types from header.
        for (size_t i = 0; i < type_names.size(); ++i)
        {
            if (column_mapping->column_indexes_for_input_fields[i] &&
                data_types[*column_mapping->column_indexes_for_input_fields[i]]->getName() != type_names[i])
            {
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Type of '{}' must be {}, not {}",
                    getPort().getHeader().getByPosition(*column_mapping->column_indexes_for_input_fields[i]).name,
                    data_types[*column_mapping->column_indexes_for_input_fields[i]]->getName(), type_names[i]);
            }
        }
    }

    if (format_settings.force_null_for_omitted_fields)
    {
        for (auto index : column_mapping->not_presented_columns)
            if (!isNullableOrLowCardinalityNullable(data_types[index]))
                throw Exception(
                    ErrorCodes::TYPE_MISMATCH,
                    "Cannot insert NULL value into a column type '{}' at index {}",
                    data_types[index]->getName(),
                    index);
    }
}

template <typename FormatReaderImpl>
void RowInputFormatWithNamesAndTypes<FormatReaderImpl>::tryDetectHeader(std::vector<String> & column_names_out, std::vector<String> & type_names_out)
{
    auto & read_buf = getReadBuffer();
    PeekableReadBuffer * peekable_buf = dynamic_cast<PeekableReadBuffer *>(&read_buf);
    if (!peekable_buf)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Header detection is supported only for formats that use PeekableReadBuffer");

    /// Empty data.
    if (unlikely(format_reader->checkForSuffix()))
    {
        end_of_stream = true;
        return;
    }

    /// Make a checkpoint before reading the first row.
    peekable_buf->setCheckpoint();
    auto first_row_values = format_reader->readRowForHeaderDetection();

    /// To understand if the first row is a header with column names, we check
    /// that all values from this row is a subset of column names from provided header
    /// or column names from provided header is a subset of values from this row
    auto column_names = getPort().getHeader().getNames();
    std::unordered_set<std::string_view> column_names_set(column_names.begin(), column_names.end());
    std::unordered_set<std::string_view> first_row_values_set(first_row_values.begin(), first_row_values.end());
    if (!isSubsetOf(first_row_values_set, column_names_set) && !isSubsetOf(column_names_set, first_row_values_set))
    {
        /// Rollback to the beginning of the first row to parse it as data later.
        peekable_buf->rollbackToCheckpoint(true);
        return;
    }

    /// First row is a header with column names.
    column_names_out = std::move(first_row_values);
    peekable_buf->dropCheckpoint();
    is_header_detected = true;

    /// Data contains only 1 row and it's just names.
    if (unlikely(format_reader->checkForSuffix()))
    {
        end_of_stream = true;
        return;
    }

    /// Make a checkpoint before reading the second row.
    peekable_buf->setCheckpoint();

    /// Skip delimiter between the first and the second rows.
    format_reader->skipRowBetweenDelimiter();
    auto second_row_values = format_reader->readRowForHeaderDetection();

    /// The second row can be a header with type names if it contains only valid type names.
    if (!checkIfAllValuesAreTypeNames(second_row_values))
    {
        /// Rollback to the beginning of the second row to parse it as data later.
        peekable_buf->rollbackToCheckpoint(true);
        return;
    }

    /// The second row is a header with type names.
    type_names_out = std::move(second_row_values);
    peekable_buf->dropCheckpoint();
}

template <typename FormatReaderImpl>
bool RowInputFormatWithNamesAndTypes<FormatReaderImpl>::readRow(MutableColumns & columns, RowReadExtension & ext)
{
    if (unlikely(end_of_stream))
        return false;

    if (unlikely(format_reader->checkForSuffix()))
    {
        end_of_stream = true;
        return false;
    }

    updateDiagnosticInfo();

    if (likely(getRowNum() != 0 || with_names || with_types || is_header_detected))
        format_reader->skipRowBetweenDelimiter();

    format_reader->skipRowStartDelimiter();

    ext.read_columns.resize(data_types.size());
    size_t file_column = 0;
    for (; file_column < column_mapping->column_indexes_for_input_fields.size(); ++file_column)
    {
        if (format_reader->allowVariableNumberOfColumns() && format_reader->checkForEndOfRow())
        {
            while (file_column < column_mapping->column_indexes_for_input_fields.size())
            {
                const auto & rem_column_index = column_mapping->column_indexes_for_input_fields[file_column];
                if (rem_column_index)
                {
                    if (format_settings.force_null_for_omitted_fields && !isNullableOrLowCardinalityNullable(data_types[*rem_column_index]))
                        throw Exception(
                            ErrorCodes::TYPE_MISMATCH,
                            "Cannot insert NULL value into a column type '{}' at index {}",
                            data_types[*rem_column_index]->getName(),
                            *rem_column_index);
                    columns[*rem_column_index]->insertDefault();
                }
                ++file_column;
            }
            break;
        }

        if (file_column != 0)
            format_reader->skipFieldDelimiter();

        const auto & column_index = column_mapping->column_indexes_for_input_fields[file_column];
        const bool is_last_file_column = file_column + 1 == column_mapping->column_indexes_for_input_fields.size();
        if (column_index)
            ext.read_columns[*column_index] = format_reader->readField(
                *columns[*column_index],
                data_types[*column_index],
                serializations[*column_index],
                is_last_file_column,
                column_mapping->names_of_columns[file_column]);
        else
            format_reader->skipField(file_column);
    }

    if (format_reader->allowVariableNumberOfColumns() && !format_reader->checkForEndOfRow())
    {
        do
        {
            format_reader->skipFieldDelimiter();
            format_reader->skipField(file_column++);
        }
        while (!format_reader->checkForEndOfRow());
    }

    format_reader->skipRowEndDelimiter();

    column_mapping->insertDefaultsForNotSeenColumns(columns, ext.read_columns);

    /// If defaults_for_omitted_fields is set to 0, we should leave already inserted defaults.
    if (!format_settings.defaults_for_omitted_fields)
        ext.read_columns.assign(ext.read_columns.size(), true);

    return true;
}

template <typename FormatReaderImpl>
size_t RowInputFormatWithNamesAndTypes<FormatReaderImpl>::countRows(size_t max_block_size)
{
    if (unlikely(end_of_stream))
        return 0;

    size_t num_rows = 0;
    bool is_first_row = getRowNum() == 0 && !with_names && !with_types && !is_header_detected;
    while (!format_reader->checkForSuffix() && num_rows < max_block_size)
    {
        if (likely(!is_first_row))
            format_reader->skipRowBetweenDelimiter();
        else
            is_first_row = false;

        format_reader->skipRow();
        ++num_rows;
    }

    if (num_rows == 0 || num_rows < max_block_size)
        end_of_stream = true;

    return num_rows;
}

template <typename FormatReaderImpl>
void RowInputFormatWithNamesAndTypes<FormatReaderImpl>::resetParser()
{
    RowInputFormatWithDiagnosticInfo::resetParser();
    column_mapping->column_indexes_for_input_fields.clear();
    column_mapping->not_presented_columns.clear();
    column_mapping->names_of_columns.clear();
    end_of_stream = false;
}

template <typename FormatReaderImpl>
void RowInputFormatWithNamesAndTypes<FormatReaderImpl>::tryDeserializeField(const DataTypePtr & type, IColumn & column, size_t file_column)
{
    const auto & index = column_mapping->column_indexes_for_input_fields[file_column];
    if (index)
    {
        format_reader->checkNullValueForNonNullable(type);
        const bool is_last_file_column = file_column + 1 == column_mapping->column_indexes_for_input_fields.size();
        format_reader->readField(column, type, serializations[*index], is_last_file_column, column_mapping->names_of_columns[file_column]);
    }
    else
    {
        format_reader->skipField(file_column);
    }
}

template <typename FormatReaderImpl>
bool RowInputFormatWithNamesAndTypes<FormatReaderImpl>::parseRowAndPrintDiagnosticInfo(MutableColumns & columns, WriteBuffer & out)
{
    if (in->eof())
    {
        out << "<End of stream>\n";
        return false;
    }

    if (!format_reader->tryParseSuffixWithDiagnosticInfo(out))
        return false;

    if (likely(getRowNum() != 0) && !format_reader->parseRowBetweenDelimiterWithDiagnosticInfo(out))
        return false;

    if (!format_reader->parseRowStartWithDiagnosticInfo(out))
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
            if (!format_reader->parseFieldDelimiterWithDiagnosticInfo(out))
                return false;
        }
    }

    return format_reader->parseRowEndWithDiagnosticInfo(out);
}

template <typename FormatReaderImpl>
bool RowInputFormatWithNamesAndTypes<FormatReaderImpl>::isGarbageAfterField(size_t index, ReadBuffer::Position pos)
{
    return format_reader->isGarbageAfterField(index, pos);
}

template <typename FormatReaderImpl>
void RowInputFormatWithNamesAndTypes<FormatReaderImpl>::setReadBuffer(ReadBuffer & in_)
{
    format_reader->setReadBuffer(in_);
    IInputFormat::setReadBuffer(in_);
}

FormatWithNamesAndTypesSchemaReader::FormatWithNamesAndTypesSchemaReader(
    ReadBuffer & in_,
    const FormatSettings & format_settings_,
    bool with_names_,
    bool with_types_,
    FormatWithNamesAndTypesReader * format_reader_,
    DataTypePtr default_type_,
    bool try_detect_header_)
    : IRowSchemaReader(in_, format_settings_, default_type_), with_names(with_names_), with_types(with_types_), format_reader(format_reader_), try_detect_header(try_detect_header_)
{
}

NamesAndTypesList FormatWithNamesAndTypesSchemaReader::readSchema()
{
    if (with_names || with_types)
        skipBOMIfExists(in);

    format_reader->skipPrefixBeforeHeader();

    std::vector<String> column_names;
    if (with_names)
        column_names = format_reader->readNames();

    std::vector<String> data_type_names;
    if (with_types)
    {
        format_reader->skipRowBetweenDelimiter();
        data_type_names = format_reader->readTypes();
    }

    if (!with_names && !with_types && try_detect_header)
        tryDetectHeader(column_names, data_type_names);

    if (!data_type_names.empty())
    {
        if (data_type_names.size() != column_names.size())
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "The number of column names {} differs with the number of types {}", column_names.size(), data_type_names.size());

        NamesAndTypesList result;
        for (size_t i = 0; i != data_type_names.size(); ++i)
            result.emplace_back(column_names[i], DataTypeFactory::instance().get(data_type_names[i]));
        return result;
    }

    if (!column_names.empty())
        setColumnNames(column_names);

    /// We should determine types by reading rows with data. Use the implementation from IRowSchemaReader.
    return IRowSchemaReader::readSchema();
}

namespace
{
    bool checkIfAllTypesAreString(const DataTypes & types)
    {
        for (const auto & type : types)
            if (!type || !isString(removeNullable(removeLowCardinality(type))))
                return false;
        return true;
    }

    bool haveNotStringAndNotNullType(const DataTypes & types)
    {
        for (const auto & type : types)
            if (type && !isString(removeNullable(removeLowCardinality(type))) && !type->onlyNull())
                return true;
        return false;
    }
}

void FormatWithNamesAndTypesSchemaReader::tryDetectHeader(std::vector<String> & column_names, std::vector<String> & type_names)
{
    auto first_row = readRowAndGetFieldsAndDataTypes();

    /// No data.
    if (!first_row)
        return;

    const auto & [first_row_values, first_row_types] = *first_row;

    /// The first row contains non String elements, it cannot be a header.
    if (!checkIfAllTypesAreString(first_row_types))
    {
        buffered_types = first_row_types;
        return;
    }

    auto second_row = readRowAndGetFieldsAndDataTypes();

    /// Data contains only 1 row, don't treat it as a header.
    if (!second_row)
    {
        buffered_types = first_row_types;
        return;
    }

    const auto & [second_row_values, second_row_types] = *second_row;

    DataTypes data_types;
    bool second_row_can_be_type_names = checkIfAllTypesAreString(second_row_types) && checkIfAllValuesAreTypeNames(readNamesFromFields(second_row_values));
    size_t row = 2;
    if (!second_row_can_be_type_names)
    {
        data_types = second_row_types;
    }
    else
    {
        auto data_types_maybe = readRowAndGetDataTypes();
        /// Data contains only 2 rows.
        if (!data_types_maybe)
        {
            second_row_can_be_type_names = false;
            data_types = second_row_types;
        }
        else
        {
            data_types = *data_types_maybe;
            ++row;
        }
    }

    /// Create default names c1,c2,... for better exception messages.
    std::vector<String> default_colum_names;
    default_colum_names.reserve(first_row_types.size());
    for (size_t i = 0; i != first_row_types.size(); ++i)
        default_colum_names.push_back("c" + std::to_string(i + 1));

    while (true)
    {
        /// Check if we have element that is not String and not Null. It means that the first two rows
        /// with all String elements are most likely a header.
        if (haveNotStringAndNotNullType(data_types))
        {
            buffered_types = data_types;
            column_names = readNamesFromFields(first_row_values);
            if (second_row_can_be_type_names)
                type_names = readNamesFromFields(second_row_values);
            return;
        }

        /// Check if we have all elements with type String. It means that the first two rows
        /// with all String elements can be real data and we cannot use them as a header.
        if (checkIfAllTypesAreString(data_types))
        {
            buffered_types = std::move(data_types);
            return;
        }

        auto next_row_types_maybe = readRowAndGetDataTypes();
        /// Check if there are no more rows in data. It means that all rows contains only String values and Nulls,
        /// so, the first two rows with all String elements can be real data and we cannot use them as a header.
        if (!next_row_types_maybe)
        {
            /// Buffer first data types from the first row, because it doesn't contain Nulls.
            buffered_types = first_row_types;
            return;
        }

        ++row;
        /// Combine types from current row and from previous rows.
        chooseResultColumnTypes(*this, data_types, *next_row_types_maybe, getDefaultDataTypeForEscapingRule(FormatSettings::EscapingRule::CSV), default_colum_names, row);
    }
}

std::optional<DataTypes> FormatWithNamesAndTypesSchemaReader::readRowAndGetDataTypes()
{
    /// Check if we tried to detect a header and have buffered types from read rows.
    if (!buffered_types.empty())
    {
        DataTypes res;
        std::swap(res, buffered_types);
        return res;
    }

    return readRowAndGetDataTypesImpl();
}

std::vector<String> FormatWithNamesAndTypesSchemaReader::readNamesFromFields(const std::vector<String> & fields)
{
    std::vector<String> names;
    names.reserve(fields.size());
    auto escaping_rule = format_reader->getEscapingRule();
    for (const auto & field : fields)
    {
        ReadBufferFromString field_buf(field);
        names.emplace_back(readStringByEscapingRule(field_buf, escaping_rule, format_settings));
    }
    return names;
}

void FormatWithNamesAndTypesSchemaReader::transformTypesIfNeeded(DB::DataTypePtr & type, DB::DataTypePtr & new_type)
{
    transformInferredTypesIfNeeded(type, new_type, format_settings);
}

template class RowInputFormatWithNamesAndTypes<JSONCompactFormatReader>;
template class RowInputFormatWithNamesAndTypes<JSONCompactEachRowFormatReader>;
template class RowInputFormatWithNamesAndTypes<TabSeparatedFormatReader>;
template class RowInputFormatWithNamesAndTypes<CSVFormatReader>;
template class RowInputFormatWithNamesAndTypes<CustomSeparatedFormatReader>;
template class RowInputFormatWithNamesAndTypes<BinaryFormatReader<true>>;
template class RowInputFormatWithNamesAndTypes<BinaryFormatReader<false>>;
}

