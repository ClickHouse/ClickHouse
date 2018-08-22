#include <Common/TaskStatsInfoGetter.h>
#include <Common/Exception.h>
#include <Core/Types.h>

#include <unistd.h>

#if defined(__linux__)

#include <common/unaligned.h>

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <syscall.h>
#include <linux/genetlink.h>
#include <linux/netlink.h>
#include <linux/taskstats.h>
#include <linux/capability.h>


/// Basic idea is motivated by "iotop" tool.
/// More info: https://www.kernel.org/doc/Documentation/accounting/taskstats.txt


namespace DB
{

namespace ErrorCodes
{
    extern const int NETLINK_ERROR;
    extern const int LOGICAL_ERROR;
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

    size_t payload_size() const
    {
        return header.nlmsg_len - sizeof(header) - sizeof(generic_header);
    }

    const Attribute * end() const
    {
        return reinterpret_cast<const Attribute *>(reinterpret_cast<const char *>(this) + header.nlmsg_len);
    }

    void send(int fd) const
    {
        const char * request_buf = reinterpret_cast<const char *>(this);
        ssize_t request_size = header.nlmsg_len;

        ::sockaddr_nl nladdr{};
        nladdr.nl_family = AF_NETLINK;

        while (true)
        {
            ssize_t bytes_sent = ::sendto(fd, request_buf, request_size, 0, reinterpret_cast<const ::sockaddr *>(&nladdr), sizeof(nladdr));

            if (bytes_sent <= 0)
            {
                if (errno == EAGAIN)
                    continue;
                else
                    throwFromErrno("Can't send a Netlink command", ErrorCodes::NETLINK_ERROR);
            }

            if (bytes_sent > request_size)
                throw Exception("Wrong result of sendto system call: bytes_sent is greater than request size", ErrorCodes::NETLINK_ERROR);

            if (bytes_sent == request_size)
                break;

            request_buf += bytes_sent;
            request_size -= bytes_sent;
        }
    }

    void receive(int fd)
    {
        ssize_t bytes_received = ::recv(fd, this, sizeof(*this), 0);

        if (header.nlmsg_type == NLMSG_ERROR || !NLMSG_OK((&header), bytes_received))
            throw Exception("Can't receive Netlink response, error: " + std::to_string(error.error), ErrorCodes::NETLINK_ERROR);
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
    NetlinkMessage request;

    request.header.nlmsg_len = NLMSG_LENGTH(GENL_HDRLEN);   /// Length of both headers.
    request.header.nlmsg_type = type;
    request.header.nlmsg_flags = NLM_F_REQUEST;             /// A request.
    request.header.nlmsg_seq = 0;
    request.header.nlmsg_pid = pid;

    request.generic_header.cmd = command;
    request.generic_header.version = 1;

    request.payload.attribute.header.nla_type = attribute_type;
    request.payload.attribute.header.nla_len = attribute_size + 1 + NLA_HDRLEN;

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
        throw Exception("Received wrong attribute as an answer to GET_FAMILY Netlink command", ErrorCodes::NETLINK_ERROR);

    return unalignedLoad<UInt16>(attr->payload);
}


bool checkPermissionsImpl()
{
    /// See man getcap.
    __user_cap_header_struct request{};
    request.version = _LINUX_CAPABILITY_VERSION_1;  /// It's enough to check just single CAP_NET_ADMIN capability we are interested.
    request.pid = getpid();

    __user_cap_data_struct response{};

    /// Avoid dependency on 'libcap'.
    if (0 != syscall(SYS_capget, &request, &response))
        throwFromErrno("Cannot do 'capget' syscall", ErrorCodes::NETLINK_ERROR);

    return (1 << CAP_NET_ADMIN) & response.effective;
}


UInt16 getFamilyId(int fd)
{
    /// It is thread and exception safe since C++11 and even before.
    static UInt16 res = getFamilyIdImpl(fd);
    return res;
}

}


bool TaskStatsInfoGetter::checkPermissions()
{
    static bool res = checkPermissionsImpl();
    return res;
}


TaskStatsInfoGetter::TaskStatsInfoGetter()
{
    if (!checkPermissions())
        throw Exception("Logical error: TaskStatsInfoGetter is not usable without CAP_NET_ADMIN. Check permissions before creating the object.",
            ErrorCodes::LOGICAL_ERROR);

    netlink_socket_fd = ::socket(PF_NETLINK, SOCK_RAW, NETLINK_GENERIC);
    if (netlink_socket_fd < 0)
        throwFromErrno("Can't create PF_NETLINK socket", ErrorCodes::NETLINK_ERROR);

    /// On some containerized environments, operation on Netlink socket could hang forever.
    /// We set reasonably small timeout to overcome this issue.

    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 50000;

    if (0 != ::setsockopt(netlink_socket_fd, SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<const char *>(&tv), sizeof(tv)))
        throwFromErrno("Can't set timeout on PF_NETLINK socket", ErrorCodes::NETLINK_ERROR);

    ::sockaddr_nl addr{};
    addr.nl_family = AF_NETLINK;

    if (::bind(netlink_socket_fd, reinterpret_cast<const ::sockaddr *>(&addr), sizeof(addr)) < 0)
        throwFromErrno("Can't bind PF_NETLINK socket", ErrorCodes::NETLINK_ERROR);

    taskstats_family_id = getFamilyId(netlink_socket_fd);
}


void TaskStatsInfoGetter::getStat(::taskstats & out_stats, pid_t tid)
{
    NetlinkMessage answer = query(netlink_socket_fd, taskstats_family_id, tid, TASKSTATS_CMD_GET, TASKSTATS_CMD_ATTR_PID, &tid, sizeof(tid));

    for (const NetlinkMessage::Attribute * attr = &answer.payload.attribute;
        attr < answer.end();
        attr = attr->next())
    {
        if (attr->header.nla_type == TASKSTATS_TYPE_AGGR_TGID || attr->header.nla_type == TASKSTATS_TYPE_AGGR_PID)
        {
            for (const NetlinkMessage::Attribute * nested_attr = reinterpret_cast<const NetlinkMessage::Attribute *>(attr->payload);
                nested_attr < attr->next();
                nested_attr = nested_attr->next())
            {
                if (nested_attr->header.nla_type == TASKSTATS_TYPE_STATS)
                {
                    out_stats = unalignedLoad<::taskstats>(nested_attr->payload);
                    return;
                }
            }
        }
    }

    throw Exception("There is no TASKSTATS_TYPE_STATS attribute in the Netlink response", ErrorCodes::NETLINK_ERROR);
}


pid_t TaskStatsInfoGetter::getCurrentTID()
{
    /// This call is always successful. - man gettid
    return static_cast<pid_t>(syscall(SYS_gettid));
}


TaskStatsInfoGetter::~TaskStatsInfoGetter()
{
    if (netlink_socket_fd >= 0)
        close(netlink_socket_fd);
}

}


#else

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

bool TaskStatsInfoGetter::checkPermissions()
{
    return false;
}


TaskStatsInfoGetter::TaskStatsInfoGetter()
{
    throw Exception("TaskStats are not implemented for this OS.", ErrorCodes::NOT_IMPLEMENTED);
}

void TaskStatsInfoGetter::getStat(::taskstats &, pid_t)
{
}

pid_t TaskStatsInfoGetter::getCurrentTID()
{
    return 0;
}

TaskStatsInfoGetter::~TaskStatsInfoGetter()
{
}

}

#endif
