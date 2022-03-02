#pragma once

#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>


namespace DB
{

class WriteBuffer;


/** Column data along with its data type and name.
  * Column data could be nullptr - to represent just 'header' of column.
  * Name could be either name from a table or some temporary generated name during expression evaluation.
  */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wnull-dereference"
struct ColumnWithTypeAndName
{
    ColumnPtr column;
    DataTypePtr type;
    String name;

    ColumnWithTypeAndName() = default;
    ColumnWithTypeAndName(ColumnPtr column_, DataTypePtr type_, String name_)
        : column(std::move(column_)), type(std::move(type_)), name(std::move(name_)) {}

    /// Uses type->createColumn() to create column
    ColumnWithTypeAndName(DataTypePtr type_, String name_)
        : column(type_->createColumn()), type(std::move(type_)), name(std::move(name_)) {}

    ColumnWithTypeAndName cloneEmpty() const;
    bool operator==(const ColumnWithTypeAndName & other) const;

    void dumpNameAndType(WriteBuffer & out) const;
    void dumpStructure(WriteBuffer & out) const;
    String dumpStructure() const;
};
#pragma GCC diagnostic pop

}
