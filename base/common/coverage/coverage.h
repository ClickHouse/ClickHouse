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
constexpr const std::string_view genhtml_command =
    "genhtml {} --output-directory {} --show-details --num-spaces 4 --legend";

struct TestData
{
    std::string test_name;
    std::vector<void*> hits;
};

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
        if (test)
            dump();

        test = DB::CurrentThread::get()

        test_id = id;
    }

    void dump()
    {
        if (!test_id)
            return;

        std::ofstream ofs(coverage_dir / std::to_string(*test_id), std::ios::binary);
        ofs << bb_edge_indices.size();

        for (const auto& e: bb_edge_indices)
            ofs << e;

        test_id = std::nullopt;
        bb_edge_indices.clear();
    }

    void writeReport()
    {
        dump();

        std::ofstream ofs(coverage_dir / "report");
        ofs << bb_edge_index_to_addr.size() << "\n";

        for (const auto& [index, addr] : bb_edge_index_to_addr)
        {
            //__sanitizer_symbolize_pc(addr, "%p %F %L", symbolizer_buffer, sizeof(symbolizer_buffer));
            ofs << index << " " << symbolizer_buffer << "\n";
        }
    }

private:
    const std::filesystem::path coverage_dir { std::filesystem::current_path() / "../../coverage" };

    std::optional<std::string> test;

    std::vector<void *> edges;
    std::mutex edges_mutex; // to prevent multithreading inserts

    std::unordered_map<void *, std::string> symbolizer_cache;
    std::shared_mutex symbolizer_cache_mutex;

    FreeThreadPool pool;
};
}
