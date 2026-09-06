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

/// True when the exception being handled is a `PrefixReadCancelledException`, i.e. a query
/// cancellation observed from inside a structure-prefix read rather than a read failure. Keyed on the
/// exception's TYPE, so nesting handlers and repeated calls all give the same answer.
bool isCancelledPrefixRead();

}
