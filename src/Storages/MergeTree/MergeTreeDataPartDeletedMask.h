#pragma once

//#include <IO/WriteBufferFromFile.h>
//#include <IO/ReadBufferFromFile.h>

#include <string_view>
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

    static constexpr std::string_view name = "deleted_mask.bin"; // {} substituted by block number

    const ColumnUInt8 & getDeletedRows() const;
    void setDeletedRows(DeletedRows new_rows);
    void setDeletedRows(size_t rows, bool value);

    void read(ReadBuffer & in);
    void write(WriteBuffer & out) const;

private:
    ColumnUInt8::Ptr deleted_rows;
};
};
