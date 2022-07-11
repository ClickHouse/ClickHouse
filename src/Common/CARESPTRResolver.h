#pragma once

#include "DNSPTRResolver.h"

using ares_channel = struct ares_channeldata *;

namespace DB {
    class CARESPTRResolver : public DNSPTRResolver {
    public:
        CARESPTRResolver();
        ~CARESPTRResolver() override;

        std::vector<std::string> resolve(const std::string & ip) override;

    private:
        void init();
        void deinit();
        void wait();

        void resolve(const std::string & ip, std::vector<std::string> & response);

        std::unique_ptr<ares_channel> channel;
    };
}

