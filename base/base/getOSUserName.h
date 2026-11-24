#pragma once

#include <string>

/// The login name of the user this process runs as, or the empty string if it cannot be
/// determined - which is not an error worth reporting anywhere this is used.
///
/// `getlogin_r` on POSIX; `GetUserNameW` on Windows, which has no `getlogin_r`.
std::string getOSUserName();
