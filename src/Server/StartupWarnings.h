#pragma once

#include <Interpreters/Context_fwd.h>

#include <ctime>
#include <string>

namespace Poco
{
class Logger;
}

namespace DB
{

struct User;

/// Warnings about how the binary was built: debug mode, sanitizers, coverage, thread fuzzer.
void addBuildWarnings(ContextPtr context);

/// Reports a partially created dedicated jemalloc arena pool for MergeTree metadata; call after the pool is initialized.
void addMergeTreeArenaPoolWarnings(ContextPtr context);

/// Warnings about the runtime environment: kernel tunables, disks, memory, dangerous settings.
/// Empty `data_path` or `logs_path` skips the corresponding disk checks.
void addEnvironmentWarnings(ContextPtr context, const Poco::Logger & logger, const std::string & data_path, const std::string & logs_path);

/// Adds or removes the warning that `default` or the user named by the `default_session_user` server setting
/// has no password and accepts connections from any host (like `default` in the shipped `users.xml`), while
/// the server listens on a non-loopback address.
/// Call after the access storages are set up, and again whenever a user or the configuration changes. Never throws.
void updateUsersWithoutPasswordWarning(ContextPtr context);

/// Whether a login of `user` with an empty password succeeds, ignoring `networks`. Applies the gates of
/// `IAccessStorage::authenticateImpl`: a method disabled by `allow_no_password` / `allow_plaintext_password` is ignored,
/// and an expired method that accepts the empty password rejects the login, even if another method accepts it too.
/// Expiry is evaluated at `now`. Exposed for tests.
bool userAcceptsEmptyPassword(const User & user, bool allow_no_password, bool allow_plaintext_password, time_t now);
}
