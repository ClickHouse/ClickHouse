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
        auto * ptr_records = static_cast<std::unordered_set<std::string>*>(arg);
        if (ptr_records && status == ARES_SUCCESS)
        {
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

    std::mutex CaresPTRResolver::mutex;

    CaresPTRResolver::CaresPTRResolver(CaresPTRResolver::provider_token) : channel(nullptr)
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

        if (library_init_result != ARES_SUCCESS || ares_init(&channel) != ARES_SUCCESS)
        {
            throw DB::Exception("Failed to initialize c-ares", DB::ErrorCodes::DNS_ERROR);
        }
    }

    CaresPTRResolver::~CaresPTRResolver()
    {
        ares_destroy(channel);
        /*
         * Library initialization is currently done only once in the constructor. Multiple instances of CaresPTRResolver
         * will be used in the lifetime of ClickHouse, thus it's problematic to have de-init here.
         * In a practical view, it makes little to no sense to de-init a DNS library since DNS requests will happen
         * until the end of the program. Hence, ares_library_cleanup() will not be called.
         * */
    }

    std::unordered_set<std::string> CaresPTRResolver::resolve(const std::string & ip)
    {
        std::lock_guard guard(mutex);

        std::unordered_set<std::string> ptr_records;

        resolve(ip, ptr_records);
        wait();

        return ptr_records;
    }

    std::unordered_set<std::string> CaresPTRResolver::resolve_v6(const std::string & ip)
    {
        std::lock_guard guard(mutex);

        std::unordered_set<std::string> ptr_records;

        resolve_v6(ip, ptr_records);
        wait();

        return ptr_records;
    }

    void CaresPTRResolver::resolve(const std::string & ip, std::unordered_set<std::string> & response)
    {
        in_addr addr;

        inet_pton(AF_INET, ip.c_str(), &addr);

        ares_gethostbyaddr(channel, reinterpret_cast<const void*>(&addr), sizeof(addr), AF_INET, callback, &response);
    }

    void CaresPTRResolver::resolve_v6(const std::string & ip, std::unordered_set<std::string> & response)
    {
        in6_addr addr;
        inet_pton(AF_INET6, ip.c_str(), &addr);

        ares_gethostbyaddr(channel, reinterpret_cast<const void*>(&addr), sizeof(addr), AF_INET6, callback, &response);
    }

    void CaresPTRResolver::wait()
    {
        int sockets[ARES_GETSOCK_MAXNUM];
        pollfd pollfd[ARES_GETSOCK_MAXNUM];

        while (true)
        {
            auto readable_sockets = get_readable_sockets(sockets, pollfd);
            auto timeout = calculate_timeout();

            int number_of_fds_ready = 0;
            if (!readable_sockets.empty())
            {
                number_of_fds_ready = poll(readable_sockets.data(), static_cast<nfds_t>(readable_sockets.size()), static_cast<int>(timeout));
            }

            if (number_of_fds_ready > 0)
            {
                process_readable_sockets(readable_sockets);
            }
            else
            {
                process_possible_timeout();
                break;
            }
        }
    }

    std::span<pollfd> CaresPTRResolver::get_readable_sockets(int * sockets, pollfd * pollfd)
    {
        int sockets_bitmask = ares_getsock(channel, sockets, ARES_GETSOCK_MAXNUM);

        int number_of_sockets_to_poll = 0;

        for (int i = 0; i < ARES_GETSOCK_MAXNUM; i++, number_of_sockets_to_poll++)
        {
            pollfd[i].events = 0;
            pollfd[i].revents = 0;

            if (ARES_GETSOCK_READABLE(sockets_bitmask, i))
            {
                pollfd[i].fd = sockets[i];
                pollfd[i].events = POLLIN;
            }
            else
            {
                break;
            }
        }

        return std::span<struct pollfd>(pollfd, number_of_sockets_to_poll);
    }

    int64_t CaresPTRResolver::calculate_timeout()
    {
        timeval tv;
        if (auto * tvp = ares_timeout(channel, nullptr, &tv))
        {
            auto timeout = tvp->tv_sec * 1000 + tvp->tv_usec / 1000;

            return timeout;
        }

        return 0;
    }

    void CaresPTRResolver::process_possible_timeout()
    {
        /* Call ares_process() unconditonally here, even if we simply timed out
        above, as otherwise the ares name resolve won't timeout! */
        ares_process_fd(channel, ARES_SOCKET_BAD, ARES_SOCKET_BAD);
    }

    void CaresPTRResolver::process_readable_sockets(std::span<pollfd> readable_sockets)
    {
        for (auto readable_socket : readable_sockets)
        {
            auto fd = readable_socket.revents & POLLIN ? readable_socket.fd : ARES_SOCKET_BAD;
            ares_process_fd(channel, fd, ARES_SOCKET_BAD);
        }
    }
}
