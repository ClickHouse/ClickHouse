#pragma once

#include <string>
#include <vector>

namespace DB {
    struct DNSPTRResolver {

        virtual ~DNSPTRResolver() = default;

        virtual std::vector<std::string> resolve(const std::string & ip) = 0;

    };
}
