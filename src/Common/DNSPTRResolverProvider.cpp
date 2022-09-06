#include "DNSPTRResolverProvider.h"
#include "CaresPTRResolver.h"

namespace DB
{
    std::shared_ptr<DNSPTRResolver> DNSPTRResolverProvider::get()
    {
        return std::make_shared<CaresPTRResolver>(
            CaresPTRResolver::provider_token {}
        );
    }
}
