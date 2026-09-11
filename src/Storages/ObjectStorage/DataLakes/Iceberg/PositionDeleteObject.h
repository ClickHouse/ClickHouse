#pragma once

#include <optional>
#include <base/types.h>

namespace DB::Iceberg
{
struct PositionDeleteObject
{
    String file_path;
    String file_format;
    std::optional<String> reference_data_file_path; // now it is always std::nullopt. Exists for compatibility reasons of the iceberg cluster function.
    Int64 sequence_number = 0;
};

struct DeletionVectorObject : PositionDeleteObject
{
    Int64 content_offset = 0;
    Int64 content_size_in_bytes = 0;
};

}
