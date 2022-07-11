#pragma once

#include <unordered_map>
#include <vector>
#include <cmath>

#include <Columns/ColumnsNumber.h>

namespace DB
{
class ReadBuffer;
class WriteBuffer;

/// Per-part info about rows deleted by lightweight mutations.
struct MergeTreeDataPartDeletedMask
{
    explicit MergeTreeDataPartDeletedMask();
    using DeletedRows = ColumnUInt8::Ptr;

    const std::string name = "deleted_rows_mask.bin";

    const ColumnUInt8 & getDeletedRows() const;
    const DeletedRows & getDeletedRowsPtr() const { return deleted_rows; }
    void setDeletedRows(DeletedRows new_rows);
    void setDeletedRows(size_t rows, bool value);

    void read(ReadBuffer & in);
    void write(WriteBuffer & out) const;

private:
    ColumnUInt8::Ptr deleted_rows;
};

};
