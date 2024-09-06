#include "CaresPTRResolver.h"
#include <arpa/inet.h>
#include <sys/select.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include "ares.h"
#include "netdb.h"

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
            auto * ptr_records = static_cast<std::unordered_set<std::string>*>(arg);
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

    struct AresChannelRAII
    {
        AresChannelRAII()
        {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
            if (ares_init(&channel) != ARES_SUCCESS)
            {
                throw DB::Exception(DB::ErrorCodes::DNS_ERROR, "Failed to initialize c-ares channel");
            }
#pragma clang diagnostic pop
        }

        ~AresChannelRAII()
        {
            ares_destroy(channel);
        }

        ares_channel channel;
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

        if (!wait_and_process(channel_raii.channel))
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

        if (!wait_and_process(channel_raii.channel))
        {
            throw DB::Exception(DB::ErrorCodes::DNS_ERROR, "Failed to complete reverse DNS query for IP {}", ip);
        }

        return ptr_records;
    }

    void CaresPTRResolver::resolve(const std::string & ip, std::unordered_set<std::string> & response, ares_channel channel)
    {
        in_addr addr;

        inet_pton(AF_INET, ip.c_str(), &addr);

        ares_gethostbyaddr(channel, reinterpret_cast<const void*>(&addr), sizeof(addr), AF_INET, callback, &response);
    }

    void CaresPTRResolver::resolve_v6(const std::string & ip, std::unordered_set<std::string> & response, ares_channel channel)
    {
        in6_addr addr;
        inet_pton(AF_INET6, ip.c_str(), &addr);

        ares_gethostbyaddr(channel, reinterpret_cast<const void*>(&addr), sizeof(addr), AF_INET6, callback, &response);
    }

    bool CaresPTRResolver::wait_and_process(ares_channel channel)
    {
        int sockets[ARES_GETSOCK_MAXNUM];
        pollfd pollfd[ARES_GETSOCK_MAXNUM];

        while (true)
        {
            auto readable_sockets = get_readable_sockets(sockets, pollfd, channel);
            auto timeout = calculate_timeout(channel);

            int number_of_fds_ready = 0;
            if (!readable_sockets.empty())
            {
                number_of_fds_ready = poll(readable_sockets.data(), static_cast<nfds_t>(readable_sockets.size()), static_cast<int>(timeout));

                bool poll_error = number_of_fds_ready < 0;
                bool is_poll_error_an_interrupt = poll_error && errno == EINTR;

                /*
                 * Retry in case of interrupts and return false in case of actual errors.
                 * */
                if (is_poll_error_an_interrupt)
                {
                    continue;
                }
                else if (poll_error)
                {
                    return false;
                }
            }

            if (number_of_fds_ready > 0)
            {
                process_readable_sockets(readable_sockets, channel);
            }
            else
            {
                process_possible_timeout(channel);
                break;
            }
        }

        return true;
    }

    std::span<pollfd> CaresPTRResolver::get_readable_sockets(int * sockets, pollfd * pollfd, ares_channel channel)
    {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
        int sockets_bitmask = ares_getsock(channel, sockets, ARES_GETSOCK_MAXNUM);
#pragma clang diagnostic pop

        int number_of_sockets_to_poll = 0;

        for (int i = 0; i < ARES_GETSOCK_MAXNUM; i++)
        {
            pollfd[i].events = 0;
            pollfd[i].revents = 0;

            if (ARES_GETSOCK_READABLE(sockets_bitmask, i))
            {
                pollfd[i].fd = sockets[i];
                pollfd[i].events = C_ARES_POLL_EVENTS;
            }

            if (pollfd[i].events)
            {
                number_of_sockets_to_poll++;
            }
            else
            {
                break;
            }
        }

        return std::span<struct pollfd>(pollfd, number_of_sockets_to_poll);
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
