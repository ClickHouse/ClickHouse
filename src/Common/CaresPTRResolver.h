#pragma once

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

    public:
        explicit CaresPTRResolver(provider_token);
        ~CaresPTRResolver() override;

        std::vector<std::string> resolve(const std::string & ip) override;

        std::vector<std::string> resolve_v6(const std::string & ip) override;

    private:
        void wait();

        void resolve(const std::string & ip, std::vector<std::string> & response);

        void resolve_v6(const std::string & ip, std::vector<std::string> & response);

        ares_channel channel;
    };
}

