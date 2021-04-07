#pragma once

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <csignal>
#include <fstream>
#include <optional>
#include <string_view>
#include <filesystem>
#include <unordered_map>
#include <vector>
#include <sanitizer/coverage_interface.h>

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

    void initialized(uint32_t count, uint32_t * /*start*/)
    {
        const struct sigaction sa = {
            .sa_sigaction =
                [](int, siginfo_t * info, auto) { Writer::instance().updateTest(info->si_value.sival_int); },
            .sa_flags = SA_SIGINFO
        };

        sigaction(SIGRTMIN + 1, &sa, nullptr);

        std::filesystem::remove_all(coverage_dir);
        std::filesystem::create_directory(coverage_dir);
    }

    void hit(uint32_t edge_index, void * addr)
    {
        bb_edge_indices.push_back(edge_index);
        bb_edge_index_to_addr.insert_or_assign(edge_index, addr);
    }

    void updateTest(size_t id)
    {
        if (test_id)
            dump();

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
            __sanitizer_symbolize_pc(addr, "%p %F %L", symbolizer_buffer, sizeof(symbolizer_buffer));
            ofs << index << " " << symbolizer_buffer << "\n";
        }
    }

private:
    char symbolizer_buffer[1024];
    const std::filesystem::path coverage_dir { std::filesystem::current_path() / "../../coverage" };

    std::optional<size_t> test_id;
    std::vector<uint32_t> bb_edge_indices;
    std::unordered_map<uint32_t, void*> bb_edge_index_to_addr;
};

inline void hit(uint32_t edge_index, void * addr) { Writer::instance().hit(edge_index, addr); }
inline void dumpReport() { Writer::instance().writeReport(); }
inline void initialized(uint32_t bb_count, uint32_t * start) { Writer::instance().initialized(bb_count, start); }
}
