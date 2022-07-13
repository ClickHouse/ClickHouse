#include "DNSPTRResolverProvider.h"
#include "CaresPTRResolver.h"

namespace DB
{
    std::shared_ptr<DNSPTRResolver> DNSPTRResolverProvider::get()
    {
        static auto cares_resolver = std::make_shared<CaresPTRResolver>(
            CaresPTRResolver::provider_token {}
        );
        return cares_resolver;
    }
}
