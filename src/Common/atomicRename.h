#pragma once
#include <string>


namespace DB
{

/// Returns true, if the following functions supported by the system
bool supportsAtomicRename(std::string * out_message = nullptr);

/// Atomically rename old_path to new_path. If new_path exists, do not overwrite it and throw exception
void renameNoReplace(const std::string & old_path, const std::string & new_path);

/// Atomically exchange oldpath and newpath. Throw exception if some of them does not exist
void renameExchange(const std::string & old_path, const std::string & new_path);

/// Returns false instead of throwing exception if renameat2 is not supported
bool renameExchangeIfSupported(const std::string & old_path, const std::string & new_path);

/// Non-atomic emulation of renameExchange for filesystems that do not support the atomic
/// exchange (renameat2 with RENAME_EXCHANGE returns EINVAL, e.g. some FUSE filesystems and
/// Docker Desktop bind mounts on macOS). Swaps the two files via a temporary name, or, if
/// new_path does not exist, just moves old_path to new_path. NOTE it's not crash-safe.
void renameExchangeNonAtomic(const std::string & old_path, const std::string & new_path);

}
