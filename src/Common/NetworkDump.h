#pragma once

#include <functional>
#include <string>


/// Allows to setup hooks for send and recv functions.
/// It is somewhat similar to strace and ltrace.

/** Example:
  * NetworkDump::initialize(
        [](ssize_t size, int, const void *, size_t, int) { fmt::print("send {}\n", size); },
        [](ssize_t size, int, void *, size_t, int) { fmt::print("recv {}\n", size); });
  */

namespace NetworkDump
{
    using SendFunction = ssize_t(int sockfd, const void * buf, size_t len, int flags);
    using RecvFunction = ssize_t(int sockfd, void * buf, size_t len, int flags);

    using SendHook = std::function<void(ssize_t, int, const void *, size_t, int)>;
    using RecvHook = std::function<void(ssize_t, int, void *, size_t, int)>;

    /// Non thread-safe. Should be called once before any network interaction.
    void initialize(SendHook send_hook, RecvHook recv_hook);

    /// Write network dump to file in RowBinary format. Calls initialize.
    void dumpToFile(const std::string & path);
}
