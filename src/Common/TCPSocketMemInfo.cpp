#include <Common/TCPSocketMemInfo.h>

#if defined(OS_LINUX) && __has_include(<linux/sock_diag.h>)

#include <linux/sock_diag.h>
#include <linux/inet_diag.h>
#include <linux/netlink.h>
#include <linux/rtnetlink.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <cstring>

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <base/scope_guard.h>

namespace DB
{

namespace
{

/// Send a SOCK_DIAG_BY_FAMILY dump request for all ESTABLISHED/CLOSE_WAIT TCP sockets.
bool sendDiagRequest(int nl_fd, uint8_t family)
{
    struct
    {
        nlmsghdr nlh;
        inet_diag_req_v2 req;
    } request{};

    request.nlh.nlmsg_len = sizeof(request);
    request.nlh.nlmsg_type = SOCK_DIAG_BY_FAMILY;
    request.nlh.nlmsg_flags = NLM_F_REQUEST | NLM_F_DUMP;
    request.req.sdiag_family = family;
    request.req.sdiag_protocol = IPPROTO_TCP;
    request.req.idiag_states = (1 << TCP_ESTABLISHED) | (1 << TCP_CLOSE_WAIT);
    request.req.idiag_ext |= (1 << (INET_DIAG_MEMINFO - 1));

    return send(nl_fd, &request, sizeof(request), 0) == static_cast<ssize_t>(sizeof(request));
}

/// Receive and parse all netlink responses, extracting inode -> TCPSocketMemInfo.
/// Uses memcpy to read headers instead of casting raw buffer pointers, avoiding -Wcast-align
/// warnings from NLMSG_NEXT / RTA_NEXT macros.
void recvDiagResponse(int nl_fd, std::unordered_map<uint64_t, TCPSocketMemInfo> & result)
{
    char buf[32768]; // NOLINT(modernize-avoid-c-arrays)

    while (true)
    {
        ssize_t len = recv(nl_fd, buf, sizeof(buf), 0);
        if (len <= 0)
            break;

        size_t remaining = static_cast<size_t>(len);
        char * ptr = buf;

        while (remaining >= sizeof(nlmsghdr))
        {
            nlmsghdr nlh;
            memcpy(&nlh, ptr, sizeof(nlh));

            if (nlh.nlmsg_len < sizeof(nlmsghdr) || nlh.nlmsg_len > remaining)
                break;

            if (nlh.nlmsg_type == NLMSG_DONE || nlh.nlmsg_type == NLMSG_ERROR)
                return;

            /// Guard against messages too short to contain inet_diag_msg.
            size_t min_msg_len = NLMSG_LENGTH(sizeof(inet_diag_msg));
            if (nlh.nlmsg_len < min_msg_len)
            {
                size_t advance = NLMSG_ALIGN(nlh.nlmsg_len);
                if (advance > remaining)
                    break;
                ptr += advance;
                remaining -= advance;
                continue;
            }

            inet_diag_msg diag_msg;
            memcpy(&diag_msg, ptr + NLMSG_HDRLEN, sizeof(diag_msg));
            uint64_t inode = diag_msg.idiag_inode;

            unsigned int attr_len = nlh.nlmsg_len - static_cast<unsigned int>(min_msg_len);
            char * attr_ptr = ptr + min_msg_len;

            while (attr_len >= sizeof(rtattr))
            {
                rtattr rta;
                memcpy(&rta, attr_ptr, sizeof(rta));

                if (rta.rta_len < sizeof(rtattr) || rta.rta_len > attr_len)
                    break;

                if (rta.rta_type == INET_DIAG_MEMINFO && rta.rta_len >= RTA_LENGTH(sizeof(inet_diag_meminfo)))
                {
                    inet_diag_meminfo mem;
                    memcpy(&mem, attr_ptr + RTA_LENGTH(0), sizeof(mem));
                    result[inode] = {.rmem = mem.idiag_rmem, .wmem = mem.idiag_wmem};
                }

                unsigned int advance = RTA_ALIGN(rta.rta_len);
                if (advance > attr_len)
                    break;
                attr_ptr += advance;
                attr_len -= advance;
            }

            size_t advance = NLMSG_ALIGN(nlh.nlmsg_len);
            if (advance > remaining)
                break;
            ptr += advance;
            remaining -= advance;
        }
    }
}

}

std::unordered_map<uint64_t, TCPSocketMemInfo> getTCPSocketMemInfoByInode()
{
    std::unordered_map<uint64_t, TCPSocketMemInfo> result;

    int nl_fd = socket(AF_NETLINK, SOCK_DGRAM | SOCK_CLOEXEC, NETLINK_SOCK_DIAG);
    if (nl_fd < 0)
        return result;

    SCOPE_EXIT({ (void)close(nl_fd); });

    /// Bind to the kernel
    sockaddr_nl addr{};
    addr.nl_family = AF_NETLINK;
    if (bind(nl_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0)
        return result;

    /// Set receive timeout so recv does not block indefinitely
    timeval tv{.tv_sec = 1, .tv_usec = 0};
    (void)setsockopt(nl_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    /// Query IPv4 sockets
    if (sendDiagRequest(nl_fd, AF_INET))
        recvDiagResponse(nl_fd, result);

    /// Query IPv6 sockets
    if (sendDiagRequest(nl_fd, AF_INET6))
        recvDiagResponse(nl_fd, result);

    return result;
}

}

#else

namespace DB
{

std::unordered_map<uint64_t, TCPSocketMemInfo> getTCPSocketMemInfoByInode()
{
    return {};
}

}

#endif
