#pragma once

#include <optional>
#include <base/types.h>

namespace DB::Iceberg
{
struct PositionDeleteObject
{
    String file_path;
    String file_format;
    /// The raw path of the data file if the delete file applies to it without row-group pruning by that path
    /// (`PositionDeleteScope::ThisFile`); a delete file with another value here is skipped.
    std::optional<String> reference_data_file_path;
    Int64 sequence_number = 0;
};
}

