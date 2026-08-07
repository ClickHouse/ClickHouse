#include <Common/DNSPTRResolverProvider.h>
#include <Common/CaresPTRResolver.h>

namespace DB
{
    std::shared_ptr<DNSPTRResolver> DNSPTRResolverProvider::get()
    {
        static auto resolver = std::make_shared<CaresPTRResolver>(
            CaresPTRResolver::provider_token {}
        );

        return resolver;
    }
}
