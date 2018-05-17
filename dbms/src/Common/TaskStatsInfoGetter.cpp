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


struct msgtemplate
{
    struct nlmsghdr n;
    struct genlmsghdr g;
    char buf[MAX_MSG_SIZE];
};


#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"

int send_cmd(int sock_fd, __u16 nlmsg_type, __u32 nlmsg_pid,
             __u8 genl_cmd, __u16 nla_type,
             void *nla_data, int nla_len)
{
    struct nlattr *na;
    struct sockaddr_nl nladdr;
    int r, buflen;
    char *buf;

    msgtemplate msg;
    memset(&msg, 0, sizeof(msg));

    msg.n.nlmsg_len = NLMSG_LENGTH(GENL_HDRLEN);
    msg.n.nlmsg_type = nlmsg_type;
    msg.n.nlmsg_flags = NLM_F_REQUEST;
    msg.n.nlmsg_seq = 0;
    msg.n.nlmsg_pid = nlmsg_pid;
    msg.g.cmd = genl_cmd;
    msg.g.version = 0x1;

    na = (struct nlattr *) GENLMSG_DATA(&msg);
    na->nla_type = nla_type;
    na->nla_len = nla_len + 1 + NLA_HDRLEN;

    memcpy(NLA_DATA(na), nla_data, nla_len);
    msg.n.nlmsg_len += NLMSG_ALIGN(na->nla_len);

    buf = (char *) &msg;
    buflen = msg.n.nlmsg_len ;
    memset(&nladdr, 0, sizeof(nladdr));
    nladdr.nl_family = AF_NETLINK;
    while ((r = sendto(sock_fd, buf, buflen, 0, (struct sockaddr *) &nladdr,
                       sizeof(nladdr))) < buflen)
    {
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


int get_family_id(int nl_sock_fd)
{
    static char name[256];

    struct
    {
        struct nlmsghdr n;
        struct genlmsghdr g;
        char buf[256];
    } ans;

    int id = 0;
    struct nlattr *na;
    int rep_len;

    strcpy(name, TASKSTATS_GENL_NAME);
    if (send_cmd(nl_sock_fd, GENL_ID_CTRL, getpid(), CTRL_CMD_GETFAMILY,
                 CTRL_ATTR_FAMILY_NAME, (void *) name,
                 strlen(TASKSTATS_GENL_NAME) + 1))
        return 0;

    rep_len = recv(nl_sock_fd, &ans, sizeof(ans), 0);
    if (ans.n.nlmsg_type == NLMSG_ERROR
            || (rep_len < 0) || !NLMSG_OK((&ans.n), rep_len))
        return 0;

    na = (struct nlattr *) GENLMSG_DATA(&ans);
    na = (struct nlattr *) ((char *) na + NLA_ALIGN(na->nla_len));
    if (na->nla_type == CTRL_ATTR_FAMILY_ID)
        id = *(__u16 *) NLA_DATA(na);

    return id;
}

bool get_taskstats(int nl_sock_fd, int nl_family_id, pid_t xxxid, struct taskstats & out_stats, Exception * out_exception = nullptr)
{
    if (send_cmd(nl_sock_fd, nl_family_id, xxxid, TASKSTATS_CMD_GET, TASKSTATS_CMD_ATTR_PID, &xxxid, sizeof(pid_t)))
        throwFromErrno("Can't send a Netlink command");

    msgtemplate msg;
    int rv = recv(nl_sock_fd, &msg, sizeof(msg), 0);

    if (msg.n.nlmsg_type == NLMSG_ERROR || !NLMSG_OK((&msg.n), rv))
    {
        struct nlmsgerr *err = static_cast<struct nlmsgerr *>(NLMSG_DATA(&msg));
        Exception e("Can't get Netlink response, error=" + std::to_string(err->error), ErrorCodes::NETLINK_ERROR);

        if (out_exception)
        {
            *out_exception = std::move(e);
            return false;
        }

        throw Exception(std::move(e));
    }

    rv = GENLMSG_PAYLOAD(&msg.n);

    struct nlattr *na = (struct nlattr *) GENLMSG_DATA(&msg);
    int len = 0;

    while (len < rv)
    {
        len += NLA_ALIGN(na->nla_len);

        if (na->nla_type == TASKSTATS_TYPE_AGGR_TGID
                || na->nla_type == TASKSTATS_TYPE_AGGR_PID)
        {
            int aggr_len = NLA_PAYLOAD(na->nla_len);
            int len2 = 0;

            na = (struct nlattr *) NLA_DATA(na);
            while (len2 < aggr_len)
            {
                if (na->nla_type == TASKSTATS_TYPE_STATS)
                {
                    struct taskstats *ts = static_cast<struct taskstats *>(NLA_DATA(na));
                    out_stats = *ts;
                }
                len2 += NLA_ALIGN(na->nla_len);
                na = (struct nlattr *) ((char *) na + len2);
            }
        }
        na = (struct nlattr *) ((char *) GENLMSG_DATA(&msg) + len);
    }

    return true;
}

#pragma GCC diagnostic pop

}


TaskStatsInfoGetter::TaskStatsInfoGetter()
{
    netlink_socket_fd = socket(PF_NETLINK, SOCK_RAW, NETLINK_GENERIC);
    if (netlink_socket_fd < 0)
        throwFromErrno("Can't create PF_NETLINK socket");

    struct sockaddr_nl addr;
    memset(&addr, 0, sizeof(addr));
    addr.nl_family = AF_NETLINK;

    if (bind(netlink_socket_fd, (struct sockaddr *) &addr, sizeof(addr)) < 0)
        throwFromErrno("Can't bind PF_NETLINK socket");

    netlink_family_id = get_family_id(netlink_socket_fd);
}

void TaskStatsInfoGetter::getStat(::taskstats & stat, int tid) const
{
    if (tid < 0)
        tid = getCurrentTID();

    get_taskstats(netlink_socket_fd, netlink_family_id, tid, stat);
}

bool TaskStatsInfoGetter::tryGetStat(::taskstats & stat, int tid) const
{
    if (tid < 0)
        tid = getCurrentTID();

    Exception e;
    return get_taskstats(netlink_socket_fd, netlink_family_id, tid, stat, &e);
}

TaskStatsInfoGetter::~TaskStatsInfoGetter()
{
    if (netlink_socket_fd > -1)
        close(netlink_socket_fd);
}

int TaskStatsInfoGetter::getCurrentTID()
{
    return static_cast<int>(syscall(SYS_gettid));
}


}
