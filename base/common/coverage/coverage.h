#pragma once

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <csignal>
#include <fstream>
#include <optional>
#include <string_view>
#include <filesystem>
#include <vector>
#include <fmt/core.h>
#include <fmt/format.h>
#include <Poco/Logger.h>
#include "common/logger_useful.h"

namespace coverage
{
class Writer
{
public:
    static Writer& instance()
    {
        static Writer w;
        return w;
    }

    void initialized(uint64_t /*bb_count*/, uint32_t * /*start*/)
    {
        struct sigaction sa = {
            .sa_sigaction =
                [](int, siginfo_t * info, auto) { Writer::instance().updateTest(info->si_value.sival_int); },
            .sa_flags = SA_SIGINFO
        };

        sigaction(Writer::signal, &sa, nullptr);

        coverage_dir = std::filesystem::current_path() / "../../coverage";

        std::filesystem::remove_all(coverage_dir);
        std::filesystem::create_directory(coverage_dir);

        LOG_INFO(log, "Initialized coverage runtime, reports dir at {}", coverage_dir.generic_string());
    }

    void hit(const void * addr) { hits.push_back(reinterpret_cast<intptr_t>(addr)); }

    void updateTest(size_t id)
    {
        if (test_id)
            dump();

        test_id = id;

        LOG_INFO(log, "Tracking coverage for test {}", id);
    }

    void dump()
    {
        if (!test_id)
        {
            LOG_INFO(log, "No data to dump");
            return;
        }

        std::ofstream ofs(coverage_dir / std::to_string(*test_id));
        ofs << fmt::format("{}", fmt::join(hits, " "));

        LOG_INFO(log, "Dumped data for test {}", *test_id);

        test_id = std::nullopt;
        hits.clear();
    }

    static constexpr auto signal = SIGUSR2;

private:
    Writer(): log(&Poco::Logger::get("Coverage"))
    {}

    Poco::Logger * log;

    std::filesystem::path coverage_dir;

    std::optional<size_t> test_id;
    std::vector<intptr_t> hits;
};

inline void hit(const void * addr) { Writer::instance().hit(addr); }
inline void dumpReport() { Writer::instance().dump(); }
inline void initialized(uint64_t bb_count, uint32_t * start) { Writer::instance().initialized(bb_count, start); }
}
