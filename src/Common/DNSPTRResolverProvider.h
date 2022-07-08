#pragma once

#include <memory>
#include "DNSPTRResolver.h"

namespace DB {
    class DNSPTRResolverProvider
    {
    public:
        static std::shared_ptr<DNSPTRResolver> get();
    };
}

