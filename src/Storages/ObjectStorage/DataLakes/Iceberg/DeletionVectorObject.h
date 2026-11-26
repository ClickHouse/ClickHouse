#pragma once

#include <base/types.h>

namespace DB::Iceberg
{
struct DeletionVectorObject
{
    String file_path;
    Int64 content_offset = 0;
    Int64 content_size_in_bytes = 0;
};
}
