#pragma once

#include <base/types.h>

namespace DB
{

/// Longest projection name a fresh DDL may accept: the name has to leave room for every form
/// derived from it, the longest of which is `delete_tmp_<name>_<block_num>.tmp_proj`.
/// Static `NAME_MAX`, not `pathconf(_PC_NAME_MAX)`, on purpose: a part may land on any disk, so a
/// limit probed from one path would not hold for the rest. Assumes `_PC_NAME_MAX >= 255` there, as
/// `DistributedSink::checkDirectoryNameLengths` does for its own path component.
size_t maxProjectionNameLength();

/// Throws ARGUMENT_OUT_OF_BOUND if the name does not fit. For fresh definitions only: a definition
/// read back from stored metadata has to keep loading.
void checkProjectionNameLength(const String & name);

/// Throws ARGUMENT_OUT_OF_BOUND naming the projection and the limit, replacing the in-flight
/// exception, but only when `directory_name` is itself the component the filesystem refused.
/// Returns for anything else, so the caller still has to rethrow.
void rethrowIfProjectionDirectoryNameTooLong(
    const String & projection_name, const String & directory_name, size_t allowed_max_length);

}
