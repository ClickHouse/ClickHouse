#include <Common/CaresPTRResolver.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <cstdlib>
#include <arpa/inet.h>
#include <sys/select.h>

#include <ares.h>
#include <netdb.h>

namespace DB
{

namespace ErrorCodes
{
extern const int DNS_ERROR;
}

static void callback(void * arg, int status, int, struct hostent * host)
{
    if (status == ARES_SUCCESS)
    {
        auto * ptr_records = static_cast<std::unordered_set<std::string> *>(arg);
        /*
             * In some cases (e.g /etc/hosts), hostent::h_name is filled and hostent::h_aliases is empty.
             * Thus, we can't rely solely on hostent::h_aliases. More info on:
             * https://github.com/ClickHouse/ClickHouse/issues/40595#issuecomment-1230526931
             * */
        if (auto * ptr_record = host->h_name)
        {
            ptr_records->insert(ptr_record);
        }

        if (host->h_aliases)
        {
            int i = 0;
            while (auto * ptr_record = host->h_aliases[i])
            {
                ptr_records->insert(ptr_record);
                i++;
            }
        }
    }
}

static void socket_state_change_callback(void * data, ares_socket_t socket_fd, int readable, int writable)
{
    dnsstate_t * state = static_cast<dnsstate_t *>(data);
    size_t idx;

    /* Find match */
    for (idx = 0; idx < state->poll_nfds; idx++)
    {
        if (state->poll_fds[idx].fd == socket_fd)
        {
            break;
        }
    }

    if (idx >= state->poll_nfds)
    {
        if (!readable && !writable)
        {
            return;
        }

        state->poll_nfds++;
        if (state->poll_nfds > state->poll_fds_alloc)
        {
            state->poll_fds_alloc = state->poll_nfds;

            {
                auto * new_poll_fds
                    = static_cast<struct pollfd *>(std::realloc(state->poll_fds, sizeof(*state->poll_fds) * state->poll_nfds));

                if (!new_poll_fds)
                    throw std::bad_alloc();

                state->poll_fds = new_poll_fds;
            }

            {
                auto * new_ares_fds
                    = static_cast<ares_fd_events_t *>(std::realloc(state->ares_fds, sizeof(*state->ares_fds) * state->poll_nfds));

                if (!new_ares_fds)
                    throw std::bad_alloc();

                state->ares_fds = new_ares_fds;
            }
        }
    }
    else
    {
        if (!readable && !writable)
        {
            memmove(&state->poll_fds[idx], &state->poll_fds[idx + 1], sizeof(*state->poll_fds) * (state->poll_nfds - idx - 1));
            state->poll_nfds--;
            return;
        }
    }

    state->poll_fds[idx].fd = socket_fd;
    state->poll_fds[idx].events = 0;
    if (readable)
    {
        state->poll_fds[idx].events |= POLLIN;
    }
    if (writable)
    {
        state->poll_fds[idx].events |= POLLOUT;
    }
}

struct AresChannelRAII
{
    AresChannelRAII()
    {
        memset(&dns_state, 0, sizeof(dns_state));

        ares_options options;
        memset(&options, 0, sizeof(options));

        options.sock_state_cb = socket_state_change_callback;
        options.sock_state_cb_data = &dns_state;

        int optmask = 0;
        optmask |= ARES_OPT_SOCK_STATE_CB;

        if (ares_init_options(&channel, &options, optmask) != ARES_SUCCESS)
        {
            throw DB::Exception(DB::ErrorCodes::DNS_ERROR, "Failed to initialize c-ares channel");
        }
    }

    ~AresChannelRAII()
    {
        ares_destroy(channel);

        free(dns_state.poll_fds);
        free(dns_state.ares_fds);
    }

    dnsstate_t & getDNSState()
    {
        return dns_state;
    }

    ares_channel channel;
    dnsstate_t dns_state;
};

CaresPTRResolver::CaresPTRResolver(CaresPTRResolver::provider_token)
{
    /*
         * ares_library_init is not thread safe. Currently, the only other usage of c-ares seems to be in grpc.
         * In grpc, ares_library_init seems to be called only in Windows.
         * See https://github.com/grpc/grpc/blob/master/src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_wrapper.cc#L1187
         * That means it's safe to init it here, but we should be cautious when introducing new code that depends on c-ares and even updates
         * to grpc. As discussed in https://github.com/ClickHouse/ClickHouse/pull/37827#discussion_r919189085, c-ares should be adapted to be atomic
         *
         * Since C++ 11 static objects are initialized in a thread safe manner. The static qualifier also makes sure
         * it'll be called/ initialized only once.
         * */
    static const auto library_init_result = ares_library_init(ARES_LIB_INIT_ALL);

    if (library_init_result != ARES_SUCCESS)
    {
        throw DB::Exception(DB::ErrorCodes::DNS_ERROR, "Failed to initialize c-ares");
    }
}

std::unordered_set<std::string> CaresPTRResolver::resolve(const std::string & ip)
{
    AresChannelRAII channel_raii;

    std::unordered_set<std::string> ptr_records;

    resolve(ip, ptr_records, channel_raii.channel);

    if (!wait_and_process(channel_raii))
    {
        throw DB::Exception(DB::ErrorCodes::DNS_ERROR, "Failed to complete reverse DNS query for IP {}", ip);
    }

    return ptr_records;
}

std::unordered_set<std::string> CaresPTRResolver::resolve_v6(const std::string & ip)
{
    AresChannelRAII channel_raii;

    std::unordered_set<std::string> ptr_records;

    resolve_v6(ip, ptr_records, channel_raii.channel);

    if (!wait_and_process(channel_raii))
    {
        throw DB::Exception(DB::ErrorCodes::DNS_ERROR, "Failed to complete reverse DNS query for IP {}", ip);
    }

    return ptr_records;
}

void CaresPTRResolver::resolve(const std::string & ip, std::unordered_set<std::string> & response, ares_channel channel)
{
    in_addr addr;

    inet_pton(AF_INET, ip.c_str(), &addr);

    ares_gethostbyaddr(channel, reinterpret_cast<const void *>(&addr), sizeof(addr), AF_INET, callback, &response);
}

void CaresPTRResolver::resolve_v6(const std::string & ip, std::unordered_set<std::string> & response, ares_channel channel)
{
    in6_addr addr;
    inet_pton(AF_INET6, ip.c_str(), &addr);

    ares_gethostbyaddr(channel, reinterpret_cast<const void *>(&addr), sizeof(addr), AF_INET6, callback, &response);
}

bool CaresPTRResolver::wait_and_process(AresChannelRAII & channel_raii)
{
    auto & dns_state = channel_raii.getDNSState();
    auto * channel = channel_raii.channel;

    while (true)
    {
        auto timeout = calculate_timeout(channel);

        int number_of_fds_ready = poll(dns_state.poll_fds, static_cast<nfds_t>(dns_state.poll_nfds), static_cast<int>(timeout));

        if (number_of_fds_ready < 0)
        {
            if (errno == EINTR)
                continue;
            return false;
        }

        if (number_of_fds_ready == 0)
        {
            process_possible_timeout(channel);
            break;
        }

        for (size_t i = 0; i < dns_state.poll_nfds; ++i)
        {
            const struct pollfd & pfd = dns_state.poll_fds[i];
            const int rfd = (pfd.revents & POLLIN) ? pfd.fd : ARES_SOCKET_BAD;
            const int wfd = (pfd.revents & POLLOUT) ? pfd.fd : ARES_SOCKET_BAD;

            if (rfd != ARES_SOCKET_BAD || wfd != ARES_SOCKET_BAD)
            {
                ares_process_fd(channel, rfd, wfd);
            }
        }
    }

    return true;
}

int64_t CaresPTRResolver::calculate_timeout(ares_channel channel)
{
    timeval tv;
    if (auto * tvp = ares_timeout(channel, nullptr, &tv))
    {
        auto timeout = tvp->tv_sec * 1000 + tvp->tv_usec / 1000;

        return timeout;
    }

    return 0;
}

void CaresPTRResolver::process_possible_timeout(ares_channel channel)
{
    /* Call ares_process() unconditionally here, even if we simply timed out
        above, as otherwise the ares name resolve won't timeout! */
    ares_process_fd(channel, ARES_SOCKET_BAD, ARES_SOCKET_BAD);
}

void CaresPTRResolver::process_readable_sockets(std::span<pollfd> readable_sockets, ares_channel channel)
{
    for (auto readable_socket : readable_sockets)
    {
        auto fd = readable_socket.revents & C_ARES_POLL_EVENTS ? readable_socket.fd : ARES_SOCKET_BAD;
        ares_process_fd(channel, fd, ARES_SOCKET_BAD);
    }
}
}
