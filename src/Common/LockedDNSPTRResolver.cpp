#include "LockedDNSPTRResolver.h"

namespace DB {

    std::mutex LockedPTRResolver::mutex;

    LockedPTRResolver::LockedPTRResolver(std::unique_ptr<DNSPTRResolver> resolver_)
    : resolver(std::move(resolver_))
    {}

    std::unordered_set<std::string> LockedPTRResolver::resolve(const std::string & ip)
    {
        std::lock_guard guard(mutex);

        return resolver->resolve(ip);
    }

    std::unordered_set<std::string> LockedPTRResolver::resolve_v6(const std::string & ip)
    {
        std::lock_guard guard(mutex);

        return resolver->resolve_v6(ip);
    }

}
