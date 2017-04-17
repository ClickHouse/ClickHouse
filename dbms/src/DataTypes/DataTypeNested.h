#pragma once

#include <DataTypes/IDataTypeDummy.h>
#include <Core/NamesAndTypes.h>


namespace DB
{

/** Stores a set of pairs (name, type) for the nested data structure.
  * Used only when creating a table. In all other cases, it is not used, since it is expanded into a set of individual columns with types.
  */
class DataTypeNested final : public IDataTypeDummy
{
private:
    /// Names and types of nested arrays.
    NamesAndTypesListPtr nested;

public:
    DataTypeNested(NamesAndTypesListPtr nested_);

    std::string getName() const override;

    DataTypePtr clone() const override
    {
        return std::make_shared<DataTypeNested>(nested);
    }

    const NamesAndTypesListPtr & getNestedTypesList() const { return nested; }

    static std::string concatenateNestedName(const std::string & nested_table_name, const std::string & nested_field_name);
    /// Returns the prefix of the name to the first '.'. Or the name is unchanged if there is no dot.
    static std::string extractNestedTableName(const std::string & nested_name);
    /// Returns the name suffix after the first dot on the right '.'. Or the name is unchanged if there is no dot.
    static std::string extractNestedColumnName(const std::string & nested_name);

    /// Creates a new list in which Nested-type columns are replaced by several columns form of `column_name.cell_name`
    static NamesAndTypesListPtr expandNestedColumns(const NamesAndTypesList & names_and_types);
};

}
