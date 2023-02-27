#pragma once

#include <Core/NamesAndTypes.h>


namespace DB
{

/// Adds default values to columns if they don't have a specified row yet.
/// This class can be useful for implementing IRowInputFormat.
/// For missing columns of nested structure, it creates not columns of empty arrays,
/// but columns of arrays of correct lengths.
class RowInputMissingColumnsFiller
{
public:
    /// Makes a column filler which checks nested structures while adding default values to columns.
    explicit RowInputMissingColumnsFiller(const NamesAndTypesList & names_and_types);
    RowInputMissingColumnsFiller(const Names & names, const DataTypes & types);
    RowInputMissingColumnsFiller(size_t count, const std::string_view * names, const DataTypePtr * types);

    /// Default constructor makes a column filler which doesn't check nested structures while
    /// adding default values to columns.
    RowInputMissingColumnsFiller();

    /// Adds default values to some columns.
    /// For each column the function checks the number of rows and if it's less than (row_num + 1)
    /// the function will add a default value to this column.
    void addDefaults(MutableColumns & columns, size_t row_num) const;

private:
    void setNestedGroups(std::unordered_map<std::string_view, std::vector<size_t>> && nested_groups, size_t num_columns);

    struct ColumnInfo
    {
        std::shared_ptr<std::vector<size_t>> nested_group;
    };
    std::vector<ColumnInfo> column_infos;
};

}
