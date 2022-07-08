#include "DNSPTRResolverProvider.h"
#include "CARESPTRResolver.h"

namespace DB {

    std::shared_ptr<DNSPTRResolver> DNSPTRResolverProvider::get()
    {
        static auto cares_resolver = std::make_shared<CARESPTRResolver>();
        return cares_resolver;
    }

}
