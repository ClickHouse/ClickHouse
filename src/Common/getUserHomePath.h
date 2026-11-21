#pragma once

#include <string>

namespace DB
{

/// The current user's home directory as a UTF-8 string, or an empty string when the environment
/// does not name one.
///
/// On POSIX this is `HOME`. On Windows it is `USERPROFILE` (or, in degenerate setups predating it,
/// `HOMEDRIVE` + `HOMEPATH`), and `HOME` only as a last resort and only when it is a Win32 path:
/// Cygwin/MSYS shells export it in POSIX form, which a native process cannot use. The values are
/// read through the wide environment, because the narrow one is encoded in the active code page
/// and would mangle a user name outside it.
std::string getUserHomePath();

/// An environment variable that names a filesystem path, as a UTF-8 string, or an empty string
/// when it is unset or empty. On Windows the value is read through the wide environment for the
/// same reason as above; on POSIX this is plain `getenv`.
std::string getPathFromEnvironment(const char * name);

}
