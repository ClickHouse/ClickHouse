#pragma once

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <csignal>
#include <fstream>
#include <memory>
#include <mutex>
#include <optional>
#include <string_view>
#include <filesystem>
#include <unordered_map>
#include <queue>
#include <vector>
#include <shared_mutex>

#include "common/types.h"
#include <Common/Dwarf.h>
#include <Common/Elf.h>
#include <Common/SymbolIndex.h>
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
            dump();
            auto lck = std::lock_guard(edges_mutex);
            test = value.get<std::string>();
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
          symbol_index(SymbolIndex::instance()),
          pool(4)
    {
        const SymbolIndex & sym_index = *symbol_index;
        const auto * object = sym_index.findObject(this); // hack to find current binary

        binary_virtual_offset = object->address_begin;
        elf = object->elf;
    }

    const std::filesystem::path coverage_dir;

    const uintptr_t binary_virtual_offset;

    const MultiVersion<SymbolIndex>::Version symbol_index;
    const std::shared_ptr<Elf> elf;

    FreeThreadPool pool;

    std::optional<std::string> test;
    std::vector<void *> edges;
    std::mutex edges_mutex; // to prevent multithreading inserts

    struct Frame
    {
        const void * virtual_addr = nullptr;
        void * physical_addr = nullptr;
        std::string symbol;
        std::string file;
        UInt64 line;
    };

    Frame symbolizeAndDemangle(const void * virtual_addr) const
    {
        Frame out = {.virtual_addr=virtual_addr};

        out.physical_addr = reinterpret_cast<void *>(uintptr_t(virtual_addr) - binary_virtual_offset);

        Dwarf::LocationInfo location;
        std::vector<Dwarf::SymbolizedFrame> inline_frames;

        assert(elf.findAddress(uintptr_t(out.physical_addr), location, Dwarf::LocationInfoMode::FAST, inline_frames));

        out.file = location.file.toString();
        out.line = location.line;

        const auto * symbol = symbol_index.findSymbol(out.virtual_addr);
        assert(symbol);

        int status = 0;
        out.symbol = demangle(symbol->name, status);
    }

    void convertToLCOVAndDumpToDisk(const std::vector<void*>& hits, std::string_view test_name)
    {
        // TN:<test name>
        // SF:<absolute path to the source file>
        // FN:<line number of function start>,<function name> for each function
        // DA:<line number>,<execution count> for each instrumented line
        // LH:<number of lines with an execution count> greater than 0, lines hit
        // LF:<number of instrumented lines>, lines found
        //
		// FNDA: <call-count>, <function-name>
		// FNF: overall count of functions, functions found
		// FNH: overall count of functions with non-zero call count, functions hit
		//
		// BRDA:<line number>,<block number>,<branch number>,<taken>
		//
		// where 'taken' is the number of times the branch was taken
		// or '-' if the block to which the branch belongs was never
		// executed

        (void)hits;
        (void)test_name;

        std::ofstream ofs(coverage_dir / test_name);

        ofs << "TN:" << test_name << "\n";

        for (const void * addr : hits)
        {
            const Frame info = symbolizeAndDemangle(addr);
            ofs << "SF:" << info.file << "\n";

            ofs << "end_of_record\n";
        }
    }
};
}
