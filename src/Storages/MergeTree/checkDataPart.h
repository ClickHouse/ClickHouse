#pragma once

#include <Storages/MergeTree/MergeTreeData.h>


namespace DB
{

/// Calculates checksums and compares them with checksums.txt.
IMergeTreeDataPart::Checksums checkDataPart(
    MergeTreeData::DataPartPtr data_part,
    bool require_checksums,
    bool & is_broken_projection,
    std::function<bool()> is_cancelled = []{ return false; },
    bool throw_on_broken_projection = false);

bool isNotEnoughMemoryErrorCode(int code);
bool isRetryableException(std::exception_ptr exception_ptr);

/// A cancelled or timed-out query says nothing about the part's health, so such an exception must
/// not reach reportBroken.
bool isQueryCancellation(std::exception_ptr exception_ptr);

}
