#pragma once

#include <optional>
#include <base/types.h>

namespace DB::Iceberg
{
enum class PositionDeleteObjectKind : uint8_t
{
    DeleteFile = 0,
    DeletionVector = 1,
};

struct PositionDeleteObject
{
    String file_path;
    String file_format;
    std::optional<String> reference_data_file_path; // now it is always std::nullopt. Exists for compatibility reasons of the iceberg cluster function.
    Int64 sequence_number = 0;
    PositionDeleteObjectKind kind = PositionDeleteObjectKind::DeleteFile;
    std::optional<Int64> content_offset;
    std::optional<Int64> content_size_in_bytes;

    bool isDeletionVector() const { return kind == PositionDeleteObjectKind::DeletionVector; }
};
}

