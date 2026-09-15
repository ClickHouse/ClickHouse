#pragma once

#include <cstdint>
#include <unordered_map>


namespace DB
{

/// Kernel memory allocated for a TCP socket's receive and transmit buffers.
struct TCPSocketMemInfo
{
    uint32_t rmem = 0; /// sk_rmem_alloc — receive buffer memory
    uint32_t wmem = 0; /// sk_wmem_alloc — transmit buffer memory
};

/// Query the kernel for buffer memory of all TCP sockets via sock_diag netlink (INET_DIAG_MEMINFO).
/// Returns a mapping from socket inode to TCPSocketMemInfo.
/// Linux-only. Returns an empty map on other platforms or on error.
std::unordered_map<uint64_t, TCPSocketMemInfo> getTCPSocketMemInfoByInode();

}
