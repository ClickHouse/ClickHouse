#include <Common/NetlinkMetricsProvider.h>
#include <Common/Exception.h>
#include <base/defines.h>
#include <base/types.h>

#include <unistd.h>

#if defined(OS_LINUX)

#include <Common/hasLinuxCapability.h>
#include <base/unaligned.h>
#include <base/getThreadId.h>
#include <Common/logger_useful.h>

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sys/socket.h>
#include <linux/genetlink.h>
#include <linux/netlink.h>
#include <linux/taskstats.h>
#include <linux/capability.h>

#pragma clang diagnostic ignored "-Wgnu-anonymous-struct"
#pragma clang diagnostic ignored "-Wnested-anon-types"

/// Basic idea is motivated by "iotop" tool.
/// More info: https://www.kernel.org/doc/Documentation/accounting/taskstats.txt


namespace DB
{

namespace ErrorCodes
{
    extern const int NETLINK_ERROR;
}

// Replace NLMSG_OK with explicit casts since that system macro contains signedness bugs which are not going to be fixed.
static inline bool is_nlmsg_ok(const struct nlmsghdr * const nlh, const ssize_t len)
{
    return len >= static_cast<ssize_t>(sizeof(*nlh)) && nlh->nlmsg_len >= sizeof(*nlh) && static_cast<size_t>(len) >= nlh->nlmsg_len;
}

namespace
{


/** The message contains:
  * - Netlink protocol header;
  * - Generic Netlink (is a sub-protocol of Netlink that we use) protocol header;
  * - Payload
  * -- that itself is a list of "Attributes" (sub-messages), each of them contains length (including header), type, and its own payload.
  * -- and attribute payload may be represented by the list of embedded attributes.
  */
struct NetlinkMessage
{
    static size_t constexpr MAX_MSG_SIZE = 1024;

    alignas(NLMSG_ALIGNTO) ::nlmsghdr header;

    struct Attribute
    {
        ::nlattr header;

        alignas(NLMSG_ALIGNTO) char payload[0];

        const Attribute * next() const
        {
            return reinterpret_cast<const Attribute *>(reinterpret_cast<const char *>(this) + NLA_ALIGN(header.nla_len));
        }
    };

    union alignas(NLMSG_ALIGNTO)
    {
        struct
        {
            ::genlmsghdr generic_header;

            union alignas(NLMSG_ALIGNTO)
            {
                char buf[MAX_MSG_SIZE];
                Attribute attribute;    /// First attribute. There may be more.
            } payload;
        };

        ::nlmsgerr error;
    };

    const Attribute * end() const
    {
        return reinterpret_cast<const Attribute *>(reinterpret_cast<const char *>(this) + header.nlmsg_len);
    }

    void send(int fd) const
    {
        const char * request_buf = reinterpret_cast<const char *>(this);
        ssize_t request_size = header.nlmsg_len;

        union
        {
            ::sockaddr_nl nladdr{};
            ::sockaddr sockaddr;
        };

        nladdr.nl_family = AF_NETLINK;

        while (true)
        {
            ssize_t bytes_sent = ::sendto(fd, request_buf, request_size, 0, &sockaddr, sizeof(nladdr));

            if (bytes_sent <= 0)
            {
                if (bytes_sent < 0 && errno == EAGAIN)
                    continue;
                throw ErrnoException(ErrorCodes::NETLINK_ERROR, "Can't send a Netlink command");
            }

            if (bytes_sent > request_size)
                throw Exception(ErrorCodes::NETLINK_ERROR, "Wrong result of sendto system call: bytes_sent is greater than request size");

            if (bytes_sent == request_size)
                break;

            request_buf += bytes_sent;
            request_size -= bytes_sent;
        }
    }

    void receive(int fd)
    {
        ssize_t bytes_received = ::recv(fd, this, sizeof(*this), 0);

        if (header.nlmsg_type == NLMSG_ERROR)
            throw Exception(ErrorCodes::NETLINK_ERROR, "Can't receive Netlink response: error {}", std::to_string(error.error));

        if (!is_nlmsg_ok(&header, bytes_received))
            throw Exception(ErrorCodes::NETLINK_ERROR, "Can't receive Netlink response: wrong number of bytes received");
    }
};


NetlinkMessage query(
    int fd,
    UInt16 type,
    UInt32 pid,
    UInt8 command,
    UInt16 attribute_type,
    const void * attribute_data,
    int attribute_size)
{
    NetlinkMessage request{};

    request.header.nlmsg_len = NLMSG_LENGTH(GENL_HDRLEN);   /// Length of both headers.
    request.header.nlmsg_type = type;
    request.header.nlmsg_flags = NLM_F_REQUEST;             /// A request.
    request.header.nlmsg_seq = 0;
    request.header.nlmsg_pid = pid;

    request.generic_header.cmd = command;
    request.generic_header.version = 1;

    request.payload.attribute.header.nla_type = attribute_type;
    request.payload.attribute.header.nla_len = attribute_size + NLA_HDRLEN;

    memcpy(&request.payload.attribute.payload, attribute_data, attribute_size);

    request.header.nlmsg_len += NLMSG_ALIGN(request.payload.attribute.header.nla_len);

    request.send(fd);

    NetlinkMessage response;
    response.receive(fd);

    return response;
}


UInt16 getFamilyIdImpl(int fd)
{
    NetlinkMessage answer = query(fd, GENL_ID_CTRL, getpid(), CTRL_CMD_GETFAMILY, CTRL_ATTR_FAMILY_NAME, TASKSTATS_GENL_NAME, strlen(TASKSTATS_GENL_NAME) + 1);

    /// NOTE Why the relevant info is located in the second attribute?
    const NetlinkMessage::Attribute * attr = answer.payload.attribute.next();

    if (attr->header.nla_type != CTRL_ATTR_FAMILY_ID)
        throw Exception(ErrorCodes::NETLINK_ERROR, "Received wrong attribute as an answer to GET_FAMILY Netlink command");

    return unalignedLoad<UInt16>(attr->payload);
}


bool checkPermissionsImpl()
{
    static bool res = hasLinuxCapability(CAP_NET_ADMIN);
    if (!res)
        return false;

    /// Check that we can successfully initialize NetlinkMetricsProvider.
    /// It will ask about family id through Netlink.
    /// On some LXC containers we have capability but we still cannot use Netlink.
    /// There is an evidence that Linux fedora-riscv 6.1.22 gives something strange instead of the expected result.

    try
    {
        ::taskstats stats{};
        NetlinkMetricsProvider().getStat(stats, static_cast<pid_t>(getThreadId()));
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::NETLINK_ERROR)
        {
            /// This error happens all the time when running inside Docker - consider it ok,
            /// don't create noise with this error.
            LOG_DEBUG(getLogger(__PRETTY_FUNCTION__), getCurrentExceptionMessageAndPattern(/* with_stacktrace */ false));
        }
        else
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
        return false;
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        return false;
    }

    return true;
}


UInt16 getFamilyId(int fd)
{
    /// It is thread and exception safe since C++11 and even before.
    static UInt16 res = getFamilyIdImpl(fd);
    return res;
}

}


bool NetlinkMetricsProvider::checkPermissions()
{
    static bool res = checkPermissionsImpl();
    return res;
}


NetlinkMetricsProvider::NetlinkMetricsProvider()
{
    netlink_socket_fd = ::socket(PF_NETLINK, SOCK_RAW, NETLINK_GENERIC);
    if (netlink_socket_fd < 0)
        throw ErrnoException(ErrorCodes::NETLINK_ERROR, "Can't create PF_NETLINK socket");

    try
    {
        /// On some containerized environments, operation on Netlink socket could hang forever.
        /// We set reasonably small timeout to overcome this issue.

        struct timeval tv;
        tv.tv_sec = 0;
        tv.tv_usec = 50000;

        if (0 != ::setsockopt(netlink_socket_fd, SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<const char *>(&tv), sizeof(tv)))
            throw ErrnoException(ErrorCodes::NETLINK_ERROR, "Can't set timeout on PF_NETLINK socket");

        union
        {
            ::sockaddr_nl addr{};
            ::sockaddr sockaddr;
        };
        addr.nl_family = AF_NETLINK;

        if (::bind(netlink_socket_fd, &sockaddr, sizeof(addr)) < 0)
            throw ErrnoException(ErrorCodes::NETLINK_ERROR, "Can't bind PF_NETLINK socket");

        taskstats_family_id = getFamilyId(netlink_socket_fd);
    }
    catch (...)
    {
        if (netlink_socket_fd >= 0)
        {
            [[maybe_unused]] int err = close(netlink_socket_fd);
            chassert(!err || errno == EINTR);
        }
        throw;
    }
}


void NetlinkMetricsProvider::getStat(::taskstats & out_stats, pid_t tid) const
{
    NetlinkMessage answer = query(netlink_socket_fd, taskstats_family_id, tid, TASKSTATS_CMD_GET, TASKSTATS_CMD_ATTR_PID, &tid, sizeof(tid));

    const NetlinkMessage::Attribute * attr = &answer.payload.attribute;
    if (attr->header.nla_type != TASKSTATS_TYPE_AGGR_PID)
        throw Exception(ErrorCodes::NETLINK_ERROR, "Expected TASKSTATS_TYPE_AGGR_PID");

    /// TASKSTATS_TYPE_AGGR_PID
    const NetlinkMessage::Attribute * nested_attr = reinterpret_cast<const NetlinkMessage::Attribute *>(attr->payload);
    if (nested_attr->header.nla_type != TASKSTATS_TYPE_PID)
        throw Exception(ErrorCodes::NETLINK_ERROR, "Expected TASKSTATS_TYPE_PID");
    if (nested_attr == nested_attr->next())
        throw Exception(ErrorCodes::NETLINK_ERROR, "No TASKSTATS_TYPE_STATS packet after TASKSTATS_TYPE_PID");
    nested_attr = nested_attr->next();
    if (nested_attr->header.nla_type != TASKSTATS_TYPE_STATS)
        throw Exception(ErrorCodes::NETLINK_ERROR, "Expected TASKSTATS_TYPE_STATS");

    out_stats = unalignedLoad<::taskstats>(nested_attr->payload);

    if (attr->next() != answer.end())
        throw Exception(ErrorCodes::NETLINK_ERROR, "Unexpected end of response");
}


NetlinkMetricsProvider::~NetlinkMetricsProvider()
{
    if (netlink_socket_fd >= 0)
    {
        [[maybe_unused]] int err = close(netlink_socket_fd);
        chassert(!err || errno == EINTR);
    }
}

}


#else

namespace DB
{

bool NetlinkMetricsProvider::checkPermissions()
{
    return false;
}

NetlinkMetricsProvider::NetlinkMetricsProvider() = default;
NetlinkMetricsProvider::~NetlinkMetricsProvider() = default;

void NetlinkMetricsProvider::getStat(::taskstats &, pid_t) const
{
}

}

#endif
