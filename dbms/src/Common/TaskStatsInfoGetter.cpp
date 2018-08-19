#include "TaskStatsInfoGetter.h"
#include <Common/Exception.h>

#include <asm/types.h>
#include <errno.h>
#include <linux/genetlink.h>
#include <linux/netlink.h>
#include <linux/taskstats.h>
#include <linux/unistd.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <syscall.h>

/// Based on: https://github.com/Tomas-M/iotop/tree/master/src
/// More info: https://www.kernel.org/doc/Documentation/accounting/taskstats.txt

/*
 * Generic macros for dealing with netlink sockets. Might be duplicated
 * elsewhere. It is recommended that commercial grade applications use
 * libnl or libnetlink and use the interfaces provided by the library
 */
#define GENLMSG_DATA(glh)       ((void *)((char*)NLMSG_DATA(glh) + GENL_HDRLEN))
#define GENLMSG_PAYLOAD(glh)    (NLMSG_PAYLOAD(glh, 0) - GENL_HDRLEN)
#define NLA_DATA(na)            ((void *)((char*)(na) + NLA_HDRLEN))
#define NLA_PAYLOAD(len)        (len - NLA_HDRLEN)


namespace DB
{

namespace ErrorCodes
{
    extern const int NETLINK_ERROR;
}


namespace
{

static size_t constexpr MAX_MSG_SIZE = 1024;


struct NetlinkMessage
{
    ::nlmsghdr n;
    ::genlmsghdr g;
    char buf[MAX_MSG_SIZE];
};


#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"

int send_cmd(int sock_fd, __u16 nlmsg_type, __u32 nlmsg_pid,
             __u8 genl_cmd, __u16 nla_type,
             void * nla_data, int nla_len) noexcept
{
    NetlinkMessage msg;
    memset(&msg, 0, sizeof(msg));

    msg.n.nlmsg_len = NLMSG_LENGTH(GENL_HDRLEN);
    msg.n.nlmsg_type = nlmsg_type;
    msg.n.nlmsg_flags = NLM_F_REQUEST;
    msg.n.nlmsg_seq = 0;
    msg.n.nlmsg_pid = nlmsg_pid;
    msg.g.cmd = genl_cmd;
    msg.g.version = 0x1;

    ::nlattr * na = static_cast<::nlattr *>(GENLMSG_DATA(&msg));
    na->nla_type = nla_type;
    na->nla_len = nla_len + 1 + NLA_HDRLEN;

    memcpy(NLA_DATA(na), nla_data, nla_len);
    msg.n.nlmsg_len += NLMSG_ALIGN(na->nla_len);

    char * buf = reinterpret_cast<char *>(&msg);
    ssize_t buflen = msg.n.nlmsg_len;

    ::sockaddr_nl nladdr;
    memset(&nladdr, 0, sizeof(nladdr));
    nladdr.nl_family = AF_NETLINK;

    while (true)
    {
        ssize_t r = ::sendto(sock_fd, buf, buflen, 0, reinterpret_cast<::sockaddr *>(&nladdr), sizeof(nladdr));

        if (r >= buflen)
            break;

        if (r > 0)
        {
            buf += r;
            buflen -= r;
        }
        else if (errno != EAGAIN)
            return -1;
    }

    return 0;
}


UInt16 get_family_id(int nl_sock_fd) noexcept
{
    struct
    {
        ::nlmsghdr n;
        ::genlmsghdr g;
        char buf[256];
    } ans;

    static char name[] = TASKSTATS_GENL_NAME;

    if (send_cmd(nl_sock_fd, GENL_ID_CTRL, getpid(), CTRL_CMD_GETFAMILY,
                 CTRL_ATTR_FAMILY_NAME, (void *) name,
                 strlen(TASKSTATS_GENL_NAME) + 1))
        return 0;

    UInt16 id = 0;
    ssize_t rep_len = ::recv(nl_sock_fd, &ans, sizeof(ans), 0);
    if (ans.n.nlmsg_type == NLMSG_ERROR || (rep_len < 0) || !NLMSG_OK((&ans.n), rep_len))
        return 0;

    ::nlattr * na;
    na = static_cast<::nlattr *>(GENLMSG_DATA(&ans));
    na = reinterpret_cast<::nlattr *>((char *) na + NLA_ALIGN(na->nla_len));
    if (na->nla_type == CTRL_ATTR_FAMILY_ID)
        id = *static_cast<__u16 *>(NLA_DATA(na));

    return id;
}

#pragma GCC diagnostic pop
}


TaskStatsInfoGetter::TaskStatsInfoGetter() = default;

void TaskStatsInfoGetter::init()
{
    if (netlink_socket_fd >= 0)
        return;

    netlink_socket_fd = ::socket(PF_NETLINK, SOCK_RAW, NETLINK_GENERIC);
    if (netlink_socket_fd < 0)
        throwFromErrno("Can't create PF_NETLINK socket");

    ::sockaddr_nl addr;
    memset(&addr, 0, sizeof(addr));
    addr.nl_family = AF_NETLINK;

    if (::bind(netlink_socket_fd, reinterpret_cast<::sockaddr *>(&addr), sizeof(addr)) < 0)
        throwFromErrno("Can't bind PF_NETLINK socket");

    netlink_family_id = get_family_id(netlink_socket_fd);
}

bool TaskStatsInfoGetter::getStatImpl(int tid, ::taskstats & out_stats, bool throw_on_error)
{
    init();

    if (send_cmd(netlink_socket_fd, netlink_family_id, tid, TASKSTATS_CMD_GET, TASKSTATS_CMD_ATTR_PID, &tid, sizeof(pid_t)))
        throwFromErrno("Can't send a Netlink command");

    NetlinkMessage msg;
    ssize_t rv = ::recv(netlink_socket_fd, &msg, sizeof(msg), 0);

    if (msg.n.nlmsg_type == NLMSG_ERROR || !NLMSG_OK((&msg.n), rv))
    {
        ::nlmsgerr * err = static_cast<::nlmsgerr *>(NLMSG_DATA(&msg));
        if (throw_on_error)
            throw Exception("Can't get Netlink response, error: " + std::to_string(err->error), ErrorCodes::NETLINK_ERROR);
        else
            return false;
    }

    rv = GENLMSG_PAYLOAD(&msg.n);

    ::nlattr * na = static_cast<::nlattr *>(GENLMSG_DATA(&msg));
    ssize_t len = 0;

    while (len < rv)
    {
        len += NLA_ALIGN(na->nla_len);

        if (na->nla_type == TASKSTATS_TYPE_AGGR_TGID || na->nla_type == TASKSTATS_TYPE_AGGR_PID)
        {
            int aggr_len = NLA_PAYLOAD(na->nla_len);
            int len2 = 0;

            na = static_cast<::nlattr *>(NLA_DATA(na));
            while (len2 < aggr_len)
            {
                if (na->nla_type == TASKSTATS_TYPE_STATS)
                {
                    ::taskstats * ts = static_cast<::taskstats *>(NLA_DATA(na));
                    out_stats = *ts;
                }

                len2 += NLA_ALIGN(na->nla_len);
                na = reinterpret_cast<::nlattr *>(reinterpret_cast<char *>(na) + len2);
            }
        }

        na = reinterpret_cast<::nlattr *>(reinterpret_cast<char *>(GENLMSG_DATA(&msg)) + len);
    }

    return true;
}

void TaskStatsInfoGetter::getStat(::taskstats & stat, int tid)
{
    tid = tid < 0 ? getDefaultTID() : tid;
    getStatImpl(tid, stat, true);
}

bool TaskStatsInfoGetter::tryGetStat(::taskstats & stat, int tid)
{
    tid = tid < 0 ? getDefaultTID() : tid;
    return getStatImpl(tid, stat, false);
}

TaskStatsInfoGetter::~TaskStatsInfoGetter()
{
    if (netlink_socket_fd >= 0)
        close(netlink_socket_fd);
}

int TaskStatsInfoGetter::getCurrentTID()
{
    /// This call is always successful. - man gettid
    return static_cast<int>(syscall(SYS_gettid));
}

int TaskStatsInfoGetter::getDefaultTID()
{
    if (default_tid < 0)
        default_tid = getCurrentTID();

    return default_tid;
}

static bool tryGetTaskStats()
{
    TaskStatsInfoGetter getter;
    ::taskstats stat;
    return getter.tryGetStat(stat);
}

bool TaskStatsInfoGetter::checkProcessHasRequiredPermissions()
{
    /// It is thread- and exception- safe since C++11
    static bool res = tryGetTaskStats();
    return res;
}

}
