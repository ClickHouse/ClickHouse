#include "CaresPTRResolver.h"
#include <arpa/inet.h>
#include <sys/select.h>
#include <Common/Exception.h>
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
        auto * ptr_records = reinterpret_cast<std::vector<std::string>*>(arg);
        if (status == ARES_SUCCESS && host->h_aliases)
        {
            int i = 0;
            while (auto * ptr_record = host->h_aliases[i])
            {
                ptr_records->emplace_back(ptr_record);
                i++;
            }
        }
    }

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

    std::vector<std::string> CaresPTRResolver::resolve(const std::string & ip)
    {
        std::vector<std::string> ptr_records;

        resolve(ip, ptr_records);
        wait();

        return ptr_records;
    }

    std::vector<std::string> CaresPTRResolver::resolve_v6(const std::string & ip)
    {
        std::vector<std::string> ptr_records;

        resolve_v6(ip, ptr_records);
        wait();

        return ptr_records;
    }

    void CaresPTRResolver::resolve(const std::string & ip, std::vector<std::string> & response)
    {
        in_addr addr;

        inet_pton(AF_INET, ip.c_str(), &addr);

        ares_gethostbyaddr(channel, reinterpret_cast<const void*>(&addr), sizeof(addr), AF_INET, callback, &response);
    }

    void CaresPTRResolver::resolve_v6(const std::string & ip, std::vector<std::string> & response)
    {
        in6_addr addr;
        inet_pton(AF_INET6, ip.c_str(), &addr);

        ares_gethostbyaddr(channel, reinterpret_cast<const void*>(&addr), sizeof(addr), AF_INET6, callback, &response);
    }

    void CaresPTRResolver::wait()
    {
        timeval * tvp, tv;
        fd_set read_fds;
        fd_set write_fds;
        int nfds;

        for (;;)
        {
            FD_ZERO(&read_fds);
            FD_ZERO(&write_fds);
            nfds = ares_fds(channel, &read_fds,&write_fds);
            if (nfds == 0)
            {
                break;
            }
            tvp = ares_timeout(channel, nullptr, &tv);
            select(nfds, &read_fds, &write_fds, nullptr, tvp);
            ares_process(channel, &read_fds, &write_fds);
        }
    }
}
