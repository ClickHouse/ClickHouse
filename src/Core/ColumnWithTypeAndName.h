#pragma once

#include <Columns/IColumn_fwd.h>
#include <base/types.h>

#include <memory>

namespace DB
{

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;
class WriteBuffer;


/** Column data along with its data type and name.
  * Column data could be nullptr - to represent just 'header' of column.
  * Name could be either name from a table or some temporary generated name during expression evaluation.
  */
struct ColumnWithTypeAndName
{
    ColumnPtr column;
    DataTypePtr type;
    String name;

    ColumnWithTypeAndName() = default;
    ColumnWithTypeAndName(const ColumnPtr & column_, const DataTypePtr & type_, const String & name_)
        : column(column_), type(type_), name(name_) {}

    /// Uses type->createColumn() to create column
    ColumnWithTypeAndName(const DataTypePtr & type_, const String & name_);

    ColumnWithTypeAndName cloneEmpty() const;
    bool operator==(const ColumnWithTypeAndName & other) const;

    void dumpNameAndType(WriteBuffer & out) const;
    void dumpStructure(WriteBuffer & out) const;
    String dumpStructure() const;
};

}
