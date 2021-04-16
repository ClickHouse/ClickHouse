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
#include <queue>
#include <vector>

#include <Common/ShellCommand.h>
#include <sanitizer/coverage_interface.h>

namespace detail
{
using ShellCommand = DB::ShellCommand;

constexpr const auto genhtml_proc_limit = 1;

constexpr const std::string_view genhtml_command =
    "genhtml {} --output-directory {} --";

class GenProcInfo
{
    FILE * const file;
    std::unique_ptr<ShellCommand> proc;

    GenProcInfo(FILE * file_)
        : file(file_), proc(ShellCommand::execute())
    {

    }

    ~GenProcInfo()
    {
        proc->wait();
        std::filesystem::remove(tmp_file_path);
    }
};

class Writer
{
public:
    static Writer& instance()
    {
        static Writer w;
        return w;
    }

    void initialized(uint32_t count, uint32_t * start)
    {
        const auto signal_hander = [](int, siginfo_t * info, auto)
        {
            Writer::instance().updateTest(info->si_value.sival_int);
        };

        const struct sigaction sa = {
            .sa_sigaction = std::move(signal_hander),
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
            //__sanitizer_symbolize_pc(addr, "%p %F %L", symbolizer_buffer, sizeof(symbolizer_buffer));
            ofs << index << " " << symbolizer_buffer << "\n";
        }
    }

private:
    char symbolizer_buffer[1024];
    const std::filesystem::path coverage_dir { std::filesystem::current_path() / "../../coverage" };

    std::optional<size_t> test_id;
    std::vector<void *> edges; //index = bb_edge index

    std::unordered_map<void *, std::string> symbolizer_cache;

    std::vector<GenProcInfo> executing_processes;
    std::queue<FILE *> waiting_processes;
};
}
