#include <Processors/Formats/ISchemaReader.h>
#include <Formats/ReadSchemaUtils.h>
#include <Formats/EscapingRuleUtils.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <boost/algorithm/string.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int ONLY_NULLS_WHILE_READING_SCHEMA;
    extern const int TYPE_MISMATCH;
    extern const int INCORRECT_DATA;
    extern const int EMPTY_DATA_PASSED;
    extern const int BAD_ARGUMENTS;
}

void chooseResultColumnType(
    DataTypePtr & type,
    DataTypePtr & new_type,
    std::function<void(DataTypePtr &, DataTypePtr &)> transform_types_if_needed,
    const DataTypePtr & default_type,
    const String & column_name,
    size_t row)
{
    if (!type)
    {
        type = new_type;
        return;
    }

    if (!new_type || type->equals(*new_type))
        return;

    transform_types_if_needed(type, new_type);
    if (type->equals(*new_type))
        return;

    /// If the new type and the previous type for this column are different,
    /// we will use default type if we have it or throw an exception.
    if (default_type)
        type = default_type;
    else
    {
        throw Exception(
            ErrorCodes::TYPE_MISMATCH,
            "Automatically defined type {} for column {} in row {} differs from type defined by previous rows: {}. "
            "You can specify the type for this column using setting schema_inference_hints",
            type->getName(),
            column_name,
            row,
            new_type->getName());
    }
}

void checkResultColumnTypeAndAppend(NamesAndTypesList & result, DataTypePtr & type, const String & name, const DataTypePtr & default_type, size_t rows_read)
{
    if (!type)
    {
        if (!default_type)
            throw Exception(
                ErrorCodes::ONLY_NULLS_WHILE_READING_SCHEMA,
                "Cannot determine type for column {} by first {} rows of data, most likely this column contains only Nulls or empty "
                "Arrays/Maps. You can specify the type for this column using setting schema_inference_hints",
                name,
                rows_read);

        type = default_type;
    }
    result.emplace_back(name, type);
}

IIRowSchemaReader::IIRowSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_, DataTypePtr default_type_)
    : ISchemaReader(in_), default_type(default_type_), hints_str(format_settings_.schema_inference_hints), format_settings(format_settings_)
{
}


void IIRowSchemaReader::setContext(ContextPtr & context)
{
    ColumnsDescription columns;
    if (tryParseColumnsListFromString(hints_str, columns, context))
    {
        for (const auto & [name, type] : columns.getAll())
            hints[name] = type;
    }
}

IRowSchemaReader::IRowSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_)
    : IIRowSchemaReader(in_, format_settings_)
{
    initColumnNames(format_settings.column_names_for_schema_inference);
}

IRowSchemaReader::IRowSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_, DataTypePtr default_type_)
    : IIRowSchemaReader(in_, format_settings_, default_type_)
{
    initColumnNames(format_settings.column_names_for_schema_inference);
}

IRowSchemaReader::IRowSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_, const DataTypes & default_types_)
    : IRowSchemaReader(in_, format_settings_)
{
    default_types = default_types_;
}

NamesAndTypesList IRowSchemaReader::readSchema()
{
    if (max_rows_to_read == 0)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot read rows to determine the schema, the maximum number of rows to read is set to 0. "
            "Most likely setting input_format_max_rows_to_read_for_schema_inference is set to 0");

    DataTypes data_types = readRowAndGetDataTypes();
    /// If column names weren't set, use default names 'c1', 'c2', ...
    if (column_names.empty())
    {
        column_names.reserve(data_types.size());
        for (size_t i = 0; i != data_types.size(); ++i)
            column_names.push_back("c" + std::to_string(i + 1));
    }
    /// If column names were set, check that the number of names match the number of types.
    else if (column_names.size() != data_types.size())
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "The number of column names {} differs with the number of types {}", column_names.size(), data_types.size());

    for (size_t i = 0; i != column_names.size(); ++i)
    {
        auto hint_it = hints.find(column_names[i]);
        if (hint_it != hints.end())
            data_types[i] = hint_it->second;
    }

    for (rows_read = 1; rows_read < max_rows_to_read; ++rows_read)
    {
        DataTypes new_data_types = readRowAndGetDataTypes();
        if (new_data_types.empty())
            /// We reached eof.
            break;

        if (new_data_types.size() != data_types.size())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Rows have different amount of values");

        for (size_t i = 0; i != data_types.size(); ++i)
        {
            /// Check if we couldn't determine the type of this column in a new row
            /// or the type for this column was taken from hints.
            if (!new_data_types[i] || hints.contains(column_names[i]))
                continue;

            auto transform_types_if_needed = [&](DataTypePtr & type, DataTypePtr & new_type){ transformTypesIfNeeded(type, new_type, i); };
            chooseResultColumnType(data_types[i], new_data_types[i], transform_types_if_needed, getDefaultType(i), std::to_string(i + 1), rows_read);
        }
    }

    /// Check that we read at list one column.
    if (data_types.empty())
        throw Exception(ErrorCodes::EMPTY_DATA_PASSED, "Cannot read rows from the data");

    NamesAndTypesList result;
    for (size_t i = 0; i != data_types.size(); ++i)
    {
        /// Check that we could determine the type of this column.
        checkResultColumnTypeAndAppend(result, data_types[i], column_names[i], getDefaultType(i), rows_read);
    }

    return result;
}

void IRowSchemaReader::initColumnNames(const String & column_names_str)
{
    if (column_names_str.empty())
        return;

    /// column_names_for_schema_inference is a string in format 'column1,column2,column3,...'
    boost::split(column_names, column_names_str, boost::is_any_of(","));
    for (auto & column_name : column_names)
    {
        std::string col_name_trimmed = boost::trim_copy(column_name);
        if (!col_name_trimmed.empty())
            column_name = col_name_trimmed;
    }
}

DataTypePtr IRowSchemaReader::getDefaultType(size_t column) const
{
    if (default_type)
        return default_type;
    if (column < default_types.size() && default_types[column])
        return default_types[column];
    return nullptr;
}

void IRowSchemaReader::transformTypesIfNeeded(DataTypePtr & type, DataTypePtr & new_type, size_t)
{
    transformInferredTypesIfNeeded(type, new_type, format_settings);
}

IRowWithNamesSchemaReader::IRowWithNamesSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_, DataTypePtr default_type_)
    : IIRowSchemaReader(in_, format_settings_, default_type_)
{
}

NamesAndTypesList IRowWithNamesSchemaReader::readSchema()
{
    if (max_rows_to_read == 0)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot read rows to determine the schema, the maximum number of rows to read is set to 0. "
            "Most likely setting input_format_max_rows_to_read_for_schema_inference is set to 0");

    bool eof = false;
    auto names_and_types = readRowAndGetNamesAndDataTypes(eof);
    std::unordered_map<String, DataTypePtr> names_to_types;
    std::vector<String> names_order;
    names_to_types.reserve(names_and_types.size());
    names_order.reserve(names_and_types.size());
    for (const auto & [name, type] : names_and_types)
    {
        auto hint_it = hints.find(name);
        if (hint_it != hints.end())
            names_to_types[name] = hint_it->second;
        else
            names_to_types[name] = type;
        names_order.push_back(name);
    }

    auto transform_types_if_needed = [&](DataTypePtr & type, DataTypePtr & new_type){ transformTypesIfNeeded(type, new_type); };
    for (rows_read = 1; rows_read < max_rows_to_read; ++rows_read)
    {
        auto new_names_and_types = readRowAndGetNamesAndDataTypes(eof);
        if (eof)
            /// We reached eof.
            break;

        for (auto & [name, new_type] : new_names_and_types)
        {
            auto it = names_to_types.find(name);
            /// If we didn't see this column before, just add it.
            if (it == names_to_types.end())
            {
                auto hint_it = hints.find(name);
                if (hint_it != hints.end())
                    names_to_types[name] = hint_it->second;
                else
                    names_to_types[name] = new_type;
                names_order.push_back(name);
                continue;
            }

            if (hints.contains(name))
                continue;

            auto & type = it->second;
            chooseResultColumnType(type, new_type, transform_types_if_needed, default_type, name, rows_read);
        }
    }

    /// Check that we read at list one column.
    if (names_to_types.empty())
        throw Exception(ErrorCodes::EMPTY_DATA_PASSED, "Cannot read rows from the data");

    NamesAndTypesList result;
    for (auto & name : names_order)
    {
        auto & type = names_to_types[name];
        /// Check that we could determine the type of this column.
        checkResultColumnTypeAndAppend(result, type, name, default_type, rows_read);
    }

    return result;
}

void IRowWithNamesSchemaReader::transformTypesIfNeeded(DataTypePtr & type, DataTypePtr & new_type)
{
    transformInferredTypesIfNeeded(type, new_type, format_settings);
}

}
