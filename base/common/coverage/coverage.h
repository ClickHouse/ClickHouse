#pragma once

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <csignal>
#include <fstream>
#include <mutex>
#include <optional>
#include <string_view>
#include <filesystem>
#include <unordered_map>
#include <queue>
#include <vector>
#include <shared_mutex>

#include <Common/ThreadPool.h>
#include <Common/CurrentThread.h>

namespace detail
{
using namespace DB;

constexpr const std::string_view genhtml_command =
    "genhtml {} --output-directory {} --show-details --num-spaces 4 --legend";

class Writer
{
public:
    static Writer& instance()
    {
        static Writer w;
        return w;
    }

    void initialized(uint32_t count, const uint32_t * start)
    {
        (void)start;

        const struct sigaction sa = { .sa_handler=[](int) { Writer::instance().updateTest(); }};

        sigaction(SIGRTMIN + 1, &sa, nullptr);

        std::filesystem::remove_all(coverage_dir);
        std::filesystem::create_directory(coverage_dir);

        edges.reserve(count);
    }

    void hit(uint32_t edge_index, void * addr)
    {
        (void)edge_index;
        std::lock_guard _(edges_mutex);
        edges.push_back(addr);
    }

    void updateTest()
    {
        dump();
        auto lck = std::lock_guard(edges_mutex);
        test = CurrentThread::get()->
    }

    void dump()
    {
        if (!test)
            return;

        pool.scheduleOrThrowOnError([this] () mutable
        {
            std::vector<void*> edges_copies;
            std::string test_name;

            {
                auto lock = std::lock_guard(edges_mutex);
                edges_copies = edges;
                test_name = *test;
                test = std::nullopt; //already copied the data, can process the new test
                edges.clear(); // hope that it's O(1).
            }

            convertToLCOVAndDumpToDisk(edges_copies, test_name);
        });
    }

private:
    void convertToLCOVAndDumpToDisk(const std::vector<void*>& hits, std::string_view test_name)
    {
        (void)hits;
        (void)test_name;
    }

    const std::filesystem::path coverage_dir { std::filesystem::current_path() / "../../coverage" };

    std::optional<std::string> test;
    std::vector<void *> edges;
    std::mutex edges_mutex; // to prevent multithreading inserts

    //std::unordered_map<void *, std::string> symbolizer_cache;
    //std::shared_mutex symbolizer_cache_mutex;

    FreeThreadPool pool;
};
}
