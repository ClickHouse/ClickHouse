#pragma once

#include <Storages/MergeTree/MergeTreeData.h>


namespace DB
{

/// Recomputed checksums of the part and of every projection whose own checksums.txt was regenerated (missing on disk).
struct CheckDataPartResult
{
    IMergeTreeDataPart::Checksums computed_checksums;
    std::vector<std::pair<String, IMergeTreeDataPart::Checksums>> computed_projections_checksums;
};

/// Calculates checksums and compares them with checksums.txt.
CheckDataPartResult checkDataPart(
    MergeTreeData::DataPartPtr data_part,
    bool require_checksums,
    bool & is_broken_projection,
    std::function<bool()> is_cancelled = []{ return false; },
    bool throw_on_broken_projection = false);

bool isNotEnoughMemoryErrorCode(int code);
bool isRetryableException(std::exception_ptr exception_ptr);

}
