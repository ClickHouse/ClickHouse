#pragma once
#include "config.h"

#include <optional>
#include <base/types.h>

namespace DB::Iceberg
{
struct PositionDeleteObject
{
    String file_path;
    String file_format;
    std::optional<String> reference_data_file_path;
};
}

