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
#include <Interpreters/Context.h>

namespace detail
{
using namespace DB;

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

        Context::setSettingHook("coverage_test_name", [this](const Field& value)
        {
            updateTest(value.get<std::string>());
        });

        std::filesystem::remove_all(coverage_dir);
        std::filesystem::create_directory(coverage_dir);

        edges.reserve(count);
    }

    void hit(uint32_t edge_index, void * addr)
    {
        (void)edge_index;
        auto lck = std::lock_guard(edges_mutex);
        edges.push_back(addr);
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
    Writer()
        : coverage_dir(std::filesystem::current_path() / "../../coverage"),
          pool(4) {}

    const std::filesystem::path coverage_dir;

    FreeThreadPool pool;

    std::optional<std::string> test;
    std::vector<void *> edges;
    std::mutex edges_mutex; // to prevent multithreading inserts

    //std::unordered_map<void *, std::string> symbolizer_cache;
    //std::shared_mutex symbolizer_cache_mutex;

    void updateTest(std::string_view new_test_name)
    {
        dump();
        auto lck = std::lock_guard(edges_mutex);
        test = new_test_name;
    }

    void convertToLCOVAndDumpToDisk(const std::vector<void*>& hits, std::string_view test_name)
    {
        (void)hits;
        (void)test_name;
    }
};
}
