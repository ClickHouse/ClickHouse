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
// TODO Current implementation is naive, may research other compression formats
struct MergeTreeDataPartDeletedMask
{
    explicit MergeTreeDataPartDeletedMask();
    using DeletedRows = ColumnUInt8::Ptr;

    static constexpr std::string_view name = "deleted_mask_{}.bin"; // {} substituted by block number
//    static constexpr std::hash<size_t> hasher;

    ColumnUInt8::Ptr deleted_rows;

    void read(ReadBuffer & in);
    void write(WriteBuffer & out) const;
};
};
