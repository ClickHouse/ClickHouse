#pragma once

#include <mutex>

#include <Common/DNSPTRResolver.h>

namespace DB {
    class LockedPTRResolver : public DNSPTRResolver
    {
    public:

        LockedPTRResolver(std::unique_ptr<DNSPTRResolver> resolver);

        std::unordered_set<std::string> resolve(const std::string & ip) override;

        std::unordered_set<std::string> resolve_v6(const std::string & ip) override;

    private:
        // this needs to be owned
        std::unique_ptr<DNSPTRResolver> resolver;

        static std::mutex mutex;
    };
}

