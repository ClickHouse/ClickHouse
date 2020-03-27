#include <Common/rename.h>
#include <Common/Exception.h>

#if defined(_GNU_SOURCE)
#include <unistd.h>
#include <sys/syscall.h>
#include <linux/fs.h>
#endif

namespace DB
{

#if defined(__NR_renameat2)
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ATOMIC_RENAME_FAIL;
    extern const int SYSTEM_ERROR;
}

static void renameat2(const std::string & old_path, const std::string & new_path, int flags)
{
    if (old_path.empty() || new_path.empty())
        throw Exception("Path is empty", ErrorCodes::LOGICAL_ERROR);
    if (old_path[0] != '/' || new_path[0] != '/')
        throw Exception("Path is relative", ErrorCodes::LOGICAL_ERROR);

    /// int olddirfd (ignored for absolute oldpath), const char *oldpath,
    /// int newdirfd (ignored for absolute newpath), const char *newpath,
    /// unsigned int flags
    if (0 == syscall(__NR_renameat2, 0, old_path.c_str(), 0, new_path.c_str(), flags))
        return;

    if (errno == EEXIST)
        throwFromErrnoWithPath("Cannot RENAME_NOREPLACE " + old_path + " to " + new_path, new_path, ErrorCodes::ATOMIC_RENAME_FAIL);
    if (errno == ENOENT)
        throwFromErrno("Paths cannot be exchanged because " + old_path + " or " + new_path + " does not exist", ErrorCodes::ATOMIC_RENAME_FAIL);
    throwFromErrnoWithPath("Cannot rename " + old_path + " to " + new_path, old_path, ErrorCodes::SYSTEM_ERROR);
}

#else
#define RENAME_NOREPLACE 0
#define RENAME_EXCHANGE 0

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
}

[[noreturn]] static void renameat2(const std::string &, const std::string &, int)
{
    throw Exception("Compiled without renameat2() support", ErrorCodes::UNSUPPORTED_METHOD);
}

#endif

#if !defined(__NR_renameat2)
[[noreturn]]
#endif
void renameNoReplace(const std::string & old_path, const std::string & new_path)
{
    renameat2(old_path, new_path, RENAME_NOREPLACE);
}

#if !defined(__NR_renameat2)
[[noreturn]]
#endif
void renameExchange(const std::string & old_path, const std::string & new_path)
{
    renameat2(old_path, new_path, RENAME_EXCHANGE);
}

}
