#include <Columns/IColumn.h>

namespace DB
{

class IColumnUnique
{
public:
    /// Column always contains Null if it's Nullable and empty string if it's String or Nullable(String).
    /// So, size may be greater than the number of inserted unique values.
    virtual ColumnPtr getColumn() const = 0;
    virtual size_t size() const { return getColumn()->size(); }

    /// Appends new value at the end of column (column's size is increased by 1).
    /// Is used to transform raw strings to Blocks (for example, inside input format parsers)
    virtual size_t insert(const Field & x) = 0;

    /// Appends range of elements from other column.
    /// Could be used to concatenate columns.
    virtual ColumnPtr insertRangeFrom(const IColumn & src, size_t start, size_t length) = 0;

    /// Appends data located in specified memory chunk if it is possible (throws an exception if it cannot be implemented).
    /// Is used to optimize some computations (in aggregation, for example).
    /// Parameter length could be ignored if column values have fixed size.
    virtual size_t insertData(const char * pos, size_t length) = 0;

    virtual size_t getInsertionPoint(const char * pos, size_t length) const = 0;

    virtual bool has(const char * pos, size_t length) const { return getInsertionPoint(pos, length) != size(); }
};

}
