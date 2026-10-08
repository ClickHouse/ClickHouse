#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Core/Block.h>
#include <Core/CaseAwareBlockNameMap.h>
#include <Formats/ColumnMapping.h>
#include <Formats/FormatSettings.h>
#include <optional>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace
{
template <typename TSearchFunc>
void addColumnsInternal(
    std::vector<String> & names_of_columns,
    ColumnMapping::OptionalIndexes & column_indexes_for_input_fields,
    std::vector<size_t> & not_presented_columns,
    const Names & column_names,
    const FormatSettings & settings,
    size_t size,
    TSearchFunc find)
{
    std::vector<bool> read_columns(size, false);

    for (const auto & name : column_names)
    {
        names_of_columns.push_back(name);

        std::optional<size_t> column_idx = find(name);
        if (!column_idx.has_value())
        {
            if (settings.skip_unknown_fields)
            {
                column_indexes_for_input_fields.push_back(std::nullopt);
                continue;
            }

            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Unknown field found in format header: "
                "'{}' at position {}\nSet the 'input_format_skip_unknown_fields' parameter explicitly "
                "to ignore and proceed",
                name,
                column_indexes_for_input_fields.size());
        }

        auto idx = column_idx.value();

        if (read_columns[idx])
            throw Exception(ErrorCodes::INCORRECT_DATA, "Duplicate field found while parsing format header: {}", name);

        read_columns[idx] = true;
        column_indexes_for_input_fields.emplace_back(column_idx);
    }

    for (size_t i = 0; i != read_columns.size(); ++i)
    {
        if (!read_columns[i])
            not_presented_columns.push_back(i);
    }
}
}

void ColumnMapping::setupByHeader(const Block & header)
{
    column_indexes_for_input_fields.resize(header.columns());
    names_of_columns = header.getNames();

    for (size_t i = 0; i < column_indexes_for_input_fields.size(); ++i)
        column_indexes_for_input_fields[i] = i;
}

void ColumnMapping::addColumns(
    const Names & column_names, const CaseAwareBlockNameMap & column_indexes_by_names, const FormatSettings & settings)
{
    addColumnsInternal(
        names_of_columns,
        column_indexes_for_input_fields,
        not_presented_columns,
        column_names,
        settings,
        column_indexes_by_names.size(),
        [&column_indexes_by_names](const std::string & name)
        {
            auto result = column_indexes_by_names.get(name);
            if (result == CaseAwareBlockNameMap::NOT_FOUND)
            {
                return std::optional<size_t>();
            }
            return std::optional<size_t>(result);
        });
}

void ColumnMapping::addColumns(const Names & column_names, const BlockNameMap & column_indexes_by_names, const FormatSettings & settings)
{
    addColumnsInternal(
        names_of_columns,
        column_indexes_for_input_fields,
        not_presented_columns,
        column_names,
        settings,
        column_indexes_by_names.size(),
        [&column_indexes_by_names](const std::string & name)
        {
            auto it = column_indexes_by_names.find(name);
            if (it == column_indexes_by_names.end())
            {
                return std::optional<size_t>();
            }
            return std::optional<size_t>(it->second);
        });
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
