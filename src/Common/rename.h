#pragma once
#include <string>

#if defined(_GNU_SOURCE)
#include <sys/syscall.h>
#endif

namespace DB
{

/// Atomically rename old_path to new_path. If new_path exists, do not overwrite it and throw exception
#if !defined(__NR_renameat2)
[[noreturn]]
#endif
void renameNoReplace(const std::string & old_path, const std::string & new_path);

/// Atomically exchange oldpath and newpath. Throw exception if some of them does not exist
#if !defined(__NR_renameat2)
[[noreturn]]
#endif
void renameExchange(const std::string & old_path, const std::string & new_path);

}
