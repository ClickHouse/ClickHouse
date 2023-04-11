#include <Formats/ColumnMapping.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

void ColumnMapping::setupByHeader(const Block & header)
{
    column_indexes_for_input_fields.resize(header.columns());
    names_of_columns = header.getNames();

    for (size_t i = 0; i < column_indexes_for_input_fields.size(); ++i)
        column_indexes_for_input_fields[i] = i;
}

void ColumnMapping::addColumns(
    const Names & column_names, const std::unordered_map<String, size_t> & column_indexes_by_names, const FormatSettings & settings)
{
    std::vector<bool> read_columns(column_indexes_by_names.size(), false);

    for (const auto & name : column_names)
    {
        names_of_columns.push_back(name);

        const auto column_it = column_indexes_by_names.find(name);
        if (column_it == column_indexes_by_names.end())
        {
            if (settings.skip_unknown_fields)
            {
                column_indexes_for_input_fields.push_back(std::nullopt);
                continue;
            }

            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Unknown field found in format header: '{}' at position {}\nSet the 'input_format_skip_unknown_fields' parameter explicitly to ignore and proceed",
                name, column_indexes_for_input_fields.size());
        }

        const auto column_index = column_it->second;

        if (read_columns[column_index])
            throw Exception("Duplicate field found while parsing format header: " + name, ErrorCodes::INCORRECT_DATA);

        read_columns[column_index] = true;
        column_indexes_for_input_fields.emplace_back(column_index);
    }

    for (size_t i = 0; i != read_columns.size(); ++i)
    {
        if (!read_columns[i])
            not_presented_columns.push_back(i);
    }
}

void ColumnMapping::insertDefaultsForNotSeenColumns(MutableColumns & columns, std::vector<UInt8> & read_columns)
{
    for (auto index : not_presented_columns)
    {
        columns[index]->insertDefault();
        read_columns[index] = false;
    }
}

}
