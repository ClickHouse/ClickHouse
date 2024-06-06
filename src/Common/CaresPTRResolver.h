#pragma once

#include <span>
#include <poll.h>
#include <mutex>
#include "DNSPTRResolver.h"

using ares_channel = struct ares_channeldata *;

namespace DB
{

    /*
     * Implements reverse DNS resolution using c-ares lib. System reverse DNS resolution via
     * gethostbyaddr or getnameinfo does not work reliably because in some systems
     * it returns all PTR records for a given IP and in others it returns only one.
     * */
    class CaresPTRResolver : public DNSPTRResolver
    {
        friend class DNSPTRResolverProvider;

        /*
         * Allow only DNSPTRProvider to instantiate this class
         * */
        struct provider_token {};

        static constexpr auto C_ARES_POLL_EVENTS = POLLRDNORM | POLLIN;

    public:
        explicit CaresPTRResolver(provider_token);

        /*
         * Library initialization is currently done only once in the constructor. Multiple instances of CaresPTRResolver
         * will be used in the lifetime of ClickHouse, thus it's problematic to have de-init here.
         * In a practical view, it makes little to no sense to de-init a DNS library since DNS requests will happen
         * until the end of the program. Hence, ares_library_cleanup() will not be called.
         * */
        ~CaresPTRResolver() override = default;

        std::unordered_set<std::string> resolve(const std::string & ip) override;

        std::unordered_set<std::string> resolve_v6(const std::string & ip) override;

    private:
        bool wait_and_process(ares_channel channel);

        void resolve(const std::string & ip, std::unordered_set<std::string> & response, ares_channel channel);

        void resolve_v6(const std::string & ip, std::unordered_set<std::string> & response, ares_channel channel);

        std::span<pollfd> get_readable_sockets(int * sockets, pollfd * pollfd, ares_channel channel);

        int64_t calculate_timeout(ares_channel channel);

        void process_possible_timeout(ares_channel channel);

        void process_readable_sockets(std::span<pollfd> readable_sockets, ares_channel channel);
    };
}

