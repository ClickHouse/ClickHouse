#pragma once

#include <string>

namespace DB
{

/// The current user's home directory as a UTF-8 string, or an empty string when the environment
/// does not name one.
///
/// `HOME` is authoritative where it is set, which includes Cygwin/MSYS shells on Windows. A
/// native Windows shell does not set it: the same idea is spelled `USERPROFILE` (or, in
/// degenerate setups predating it, `HOMEDRIVE` + `HOMEPATH`), so those are consulted next there -
/// read through the wide environment, because the narrow one is encoded in the active code page
/// and would mangle a user name outside it.
std::string getUserHomePath();

/// An environment variable that names a filesystem path, as a UTF-8 string, or an empty string
/// when it is unset or empty. On Windows the value is read through the wide environment for the
/// same reason as above; on POSIX this is plain `getenv`.
std::string getPathFromEnvironment(const char * name);

}
