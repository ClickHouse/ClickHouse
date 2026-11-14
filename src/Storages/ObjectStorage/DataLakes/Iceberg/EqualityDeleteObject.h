#pragma once
#include "config.h"

#include <optional>
#include <base/types.h>
#include <vector>

namespace DB::Iceberg
{
struct EqualityDeleteObject
{
    String file_path;
    String file_format;
    std::optional<std::vector<Int32>> equality_ids;
    Int32 schema_id;
};
}
