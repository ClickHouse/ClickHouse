#pragma once

#include <memory>
#include "DNSPTRResolver.h"

namespace DB
{
    /*
     * Provides a ready-to-use DNSPTRResolver instance.
     * It hides 3rd party lib dependencies, handles initialization and lifetime.
     * Since `get` function is static, it can be called from any context. Including cached static functions.
     * */
    class DNSPTRResolverProvider
    {
    public:
        static std::shared_ptr<DNSPTRResolver> get();
    };
}
