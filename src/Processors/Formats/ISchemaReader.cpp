#include <Processors/Formats/ISchemaReader.h>
#include <Formats/ReadSchemaUtils.h>
#include <DataTypes/DataTypeString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
}

IRowSchemaReader::IRowSchemaReader(ReadBuffer & in_, size_t max_rows_to_read_, DataTypePtr default_type_)
    : ISchemaReader(in_), max_rows_to_read(max_rows_to_read_), default_type(default_type_)
{
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

            /// If we couldn't determine the type of column yet, just set the new type.
            if (!data_types[i])
                data_types[i] = new_data_types[i];
            /// If the new type and the previous type for this column are different,
            /// we will use default type if we have it or throw an exception.
            else if (data_types[i]->getName() != new_data_types[i]->getName())
            {
                if (default_type)
                    data_types[i] = default_type;
                else
                    throw Exception(
                        ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                        "Automatically defined type {} for column {} in row {} differs from type defined by previous rows: {}", new_data_types[i]->getName(), i + 1, row, data_types[i]->getName());
            }
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
        if (!data_types[i])
        {
            if (!default_type)
                throw Exception(
                    ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                    "Cannot determine table structure by first {} rows of data, because some columns contain only Nulls. To increase the maximum "
                    "number of rows to read for structure determination, use setting input_format_max_rows_to_read_for_schema_inference",
                    max_rows_to_read);

            data_types[i] = default_type;
        }
        result.emplace_back(column_names[i], data_types[i]);
    }

    return result;
}

IRowWithNamesSchemaReader::IRowWithNamesSchemaReader(ReadBuffer & in_, size_t max_rows_to_read_, DataTypePtr default_type_)
    : ISchemaReader(in_), max_rows_to_read(max_rows_to_read_), default_type(default_type_)
{
}

NamesAndTypesList IRowWithNamesSchemaReader::readSchema()
{
    auto names_and_types = readRowAndGetNamesAndDataTypes();
    for (size_t row = 1; row < max_rows_to_read; ++row)
    {
        auto new_names_and_types = readRowAndGetNamesAndDataTypes();
        if (new_names_and_types.empty())
            /// We reached eof.
            break;

        for (const auto & [name, new_type] : new_names_and_types)
        {
            auto it = names_and_types.find(name);
            /// If we didn't see this column before, just add it.
            if (it == names_and_types.end())
            {
                names_and_types[name] = new_type;
                continue;
            }

            auto & type = it->second;
            /// If we couldn't determine the type of column yet, just set the new type.
            if (!type)
                type = new_type;
            /// If the new type and the previous type for this column are different,
            /// we will use default type if we have it or throw an exception.
            else if (new_type && type->getName() != new_type->getName())
            {
                if (default_type)
                    type = default_type;
                else
                    throw Exception(
                        ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                        "Automatically defined type {} for column {} in row {} differs from type defined by previous rows: {}", type->getName(), name, row, new_type->getName());
            }
        }
    }

    /// Check that we read at list one column.
    if (names_and_types.empty())
        throw Exception(ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE, "Cannot read rows from the data");

    NamesAndTypesList result;
    for (auto & [name, type] : names_and_types)
    {
        /// Check that we could determine the type of this column.
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

    return result;
}

}
