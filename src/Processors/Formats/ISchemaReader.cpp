#include <Processors/Formats/ISchemaReader.h>
#include <Formats/ReadSchemaUtils.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <boost/algorithm/string.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
}

static void chooseResultType(
    DataTypePtr & type,
    const DataTypePtr & new_type,
    bool allow_bools_as_numbers,
    const DataTypePtr & default_type,
    const String & column_name,
    size_t row)
{
    if (!type)
        type = new_type;

    /// If the new type and the previous type for this column are different,
    /// we will use default type if we have it or throw an exception.
    if (new_type && !type->equals(*new_type))
    {
        /// Check if we have Bool and Number and if allow_bools_as_numbers
        /// is true make the result type Number
        auto not_nullable_type = removeNullable(type);
        auto not_nullable_new_type = removeNullable(new_type);
        bool bool_type_presents = isBool(not_nullable_type) || isBool(not_nullable_new_type);
        bool number_type_presents = isNumber(not_nullable_type) || isNumber(not_nullable_new_type);
        if (allow_bools_as_numbers && bool_type_presents && number_type_presents)
        {
            if (isBool(not_nullable_type))
                type = new_type;
        }
        else if (default_type)
            type = default_type;
        else
            throw Exception(
                ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                "Automatically defined type {} for column {} in row {} differs from type defined by previous rows: {}",
                type->getName(),
                column_name,
                row,
                new_type->getName());
    }
}

static void checkTypeAndAppend(NamesAndTypesList & result, DataTypePtr & type, const String & name, const DataTypePtr & default_type, size_t max_rows_to_read)
{
    if (!type)
    {
        if (!default_type)
            throw Exception(
                ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                "Cannot determine table structure by first {} rows of data, because some columns contain only Nulls. To increase the maximum "
                "number of rows to read for structure determination, use setting input_format_max_rows_to_read_for_schema_inference",
                max_rows_to_read);

        type = default_type;
    }
    result.emplace_back(name, type);
}

IRowSchemaReader::IRowSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings, bool allow_bools_as_numbers_)
    : ISchemaReader(in_), max_rows_to_read(format_settings.max_rows_to_read_for_schema_inference), allow_bools_as_numbers(allow_bools_as_numbers_)
{
    if (!format_settings.column_names_for_schema_inference.empty())
    {
        /// column_names_for_schema_inference is a string in format 'column1,column2,column3,...'
        boost::split(column_names, format_settings.column_names_for_schema_inference, boost::is_any_of(","));
        for (auto & column_name : column_names)
        {
            std::string col_name_trimmed = boost::trim_copy(column_name);
            if (!col_name_trimmed.empty())
                column_name = col_name_trimmed;
        }
    }
}

IRowSchemaReader::IRowSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings, DataTypePtr default_type_, bool allow_bools_as_numbers_)
    : IRowSchemaReader(in_, format_settings, allow_bools_as_numbers_)
{
    default_type = default_type_;
}

IRowSchemaReader::IRowSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings, const DataTypes & default_types_, bool allow_bools_as_numbers_)
    : IRowSchemaReader(in_, format_settings, allow_bools_as_numbers_)
{
    default_types = default_types_;
}

NamesAndTypesList IRowSchemaReader::readSchema()
{
    DataTypes data_types = readRowAndGetDataTypes();
    for (size_t row = 1; row < max_rows_to_read; ++row)
    {
        DataTypes new_data_types = readRowAndGetDataTypes();
        if (new_data_types.empty())
            /// We reached eof.
            break;

        if (new_data_types.size() != data_types.size())
            throw Exception(ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE, "Rows have different amount of values");

        for (size_t i = 0; i != data_types.size(); ++i)
        {
            /// We couldn't determine the type of this column in a new row, just skip it.
            if (!new_data_types[i])
                continue;

            chooseResultType(data_types[i], new_data_types[i], allow_bools_as_numbers, getDefaultType(i), std::to_string(i + 1), row);
        }
    }

    /// Check that we read at list one column.
    if (data_types.empty())
        throw Exception(ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE, "Cannot read rows from the data");

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
            ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
            "The number of column names {} differs with the number of types {}", column_names.size(), data_types.size());

    NamesAndTypesList result;
    for (size_t i = 0; i != data_types.size(); ++i)
    {
        /// Check that we could determine the type of this column.
        checkTypeAndAppend(result, data_types[i], column_names[i], getDefaultType(i), max_rows_to_read);
    }

    return result;
}

DataTypePtr IRowSchemaReader::getDefaultType(size_t column) const
{
    if (default_type)
        return default_type;
    if (column < default_types.size() && default_types[column])
        return default_types[column];
    return nullptr;
}

IRowWithNamesSchemaReader::IRowWithNamesSchemaReader(ReadBuffer & in_, size_t max_rows_to_read_, DataTypePtr default_type_, bool allow_bools_as_numbers_)
    : ISchemaReader(in_), max_rows_to_read(max_rows_to_read_), default_type(default_type_), allow_bools_as_numbers(allow_bools_as_numbers_)
{
}

NamesAndTypesList IRowWithNamesSchemaReader::readSchema()
{
    bool eof = false;
    auto names_and_types = readRowAndGetNamesAndDataTypes(eof);
    std::unordered_map<String, DataTypePtr> names_to_types;
    std::vector<String> names_order;
    names_to_types.reserve(names_and_types.size());
    names_order.reserve(names_and_types.size());
    for (const auto & [name, type] : names_and_types)
    {
        names_to_types[name] = type;
        names_order.push_back(name);
    }

    for (size_t row = 1; row < max_rows_to_read; ++row)
    {
        auto new_names_and_types = readRowAndGetNamesAndDataTypes(eof);
        if (eof)
            /// We reached eof.
            break;

        for (const auto & [name, new_type] : new_names_and_types)
        {
            auto it = names_to_types.find(name);
            /// If we didn't see this column before, just add it.
            if (it == names_to_types.end())
            {
                names_to_types[name] = new_type;
                names_order.push_back(name);
                continue;
            }

            auto & type = it->second;
            chooseResultType(type, new_type, allow_bools_as_numbers, default_type, name, row);
        }
    }

    /// Check that we read at list one column.
    if (names_to_types.empty())
        throw Exception(ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE, "Cannot read rows from the data");

    NamesAndTypesList result;
    for (auto & name : names_order)
    {
        auto & type = names_to_types[name];
        /// Check that we could determine the type of this column.
        checkTypeAndAppend(result, type, name, default_type, max_rows_to_read);
    }

    return result;
}

}
