#pragma once

#include <optional>
#include <base/types.h>

namespace DB::Iceberg
{
struct PositionDeleteObject
{
    String file_path;
    String file_format;
    /// The raw data file path for `PositionDeleteScope::ThisFile`; a delete file with another value here is skipped.
    std::optional<String> reference_data_file_path;
    Int64 sequence_number = 0;
};
}

