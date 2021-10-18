#include "MergeTreeDataPartDeletedMask.h"
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB::ErrorCodes
{
    extern const int UNKNOWN_FORMAT_VERSION;
    extern const int HASH_MISMATCH_FOR_DELETED_MASK;
}

namespace DB
{
void MergeTreeDataPartDeletedMask::read(ReadBuffer & in)
{
    size_t format_version;

    readIntBinary(format_version, in);

    if (format_version != 1)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION,
            "Unknown format version {} for deleted mask", format_version);

    readIntBinary(deleted_rows_hash, in);
    readVectorBinary(deleted_rows, in);

    size_t real_hash = 0;

    for (size_t elem : deleted_rows)
        real_hash ^= hasher(elem);

    if (deleted_rows_hash != real_hash)
        throw Exception(ErrorCodes::HASH_MISMATCH_FOR_DELETED_MASK,
            "Hash mismatch for deleted mask: expected {}, found {}",
            deleted_rows_hash, real_hash);

    assertEOF(in);
}

void MergeTreeDataPartDeletedMask::write(WriteBuffer & out) const
{
    writeVarUInt(1, out); /// Format version
    writeVarUInt(deleted_rows_hash, out);
    writeBinary(std::span{deleted_rows}, out);
}
}
