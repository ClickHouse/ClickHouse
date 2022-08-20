#pragma once

#include <string>
#include <vector>

namespace DB
{
    struct DNSPTRResolver
    {

        virtual ~DNSPTRResolver() = default;

        virtual std::vector<std::string> resolve(const std::string & ip) = 0;

        virtual std::vector<std::string> resolve_v6(const std::string & ip) = 0;

    };
}
