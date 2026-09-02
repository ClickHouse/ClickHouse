#pragma once

#include <string>
#include <unordered_set>

namespace DB
{
    struct DNSPTRResolver
    {

        virtual ~DNSPTRResolver() = default;

        virtual std::unordered_set<std::string> resolve(const std::string & ip) = 0;

        virtual std::unordered_set<std::string> resolve_v6(const std::string & ip) = 0;

    };
}
