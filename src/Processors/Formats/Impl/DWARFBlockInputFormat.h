#pragma once
#include "config.h"
#if USE_DWARF_PARSER && defined(__ELF__) && !defined(OS_FREEBSD)

#include <llvm/DebugInfo/DWARF/DWARFDebugAbbrev.h>
#include <llvm/DebugInfo/DWARF/DWARFDataExtractor.h>
#include <llvm/DebugInfo/DWARF/DWARFContext.h>
#include <llvm/DebugInfo/DWARF/DWARFUnit.h>

#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>
#include <Common/Elf.h>
#include <Common/ThreadPool.h>
#include <Columns/ColumnVector.h>

namespace DB
{

class DWARFBlockInputFormat : public IInputFormat
{
public:
    DWARFBlockInputFormat(ReadBuffer & in_, Block header_, const FormatSettings & format_settings_, size_t num_threads_);
    ~DWARFBlockInputFormat() override;

    String getName() const override { return "DWARFBlockInputFormat"; }

    void resetParser() override;

    size_t getApproxBytesReadForChunk() const override { return approx_bytes_read_for_chunk; }

protected:
    Chunk generate() override;

    void onCancel() override
    {
        is_stopped = 1;
    }

private:
    struct StackEntry
    {
        uint64_t offset;
        llvm::dwarf::Tag tag;
    };

    struct UnitState
    {
        const llvm::DWARFUnit * dwarf_unit;
        const llvm::DWARFAbbreviationDeclarationSet * abbrevs;
        uint64_t end_offset;

        std::string unit_name;
        ColumnPtr filename_table; // from .debug_line
        size_t filename_table_size = 0;
        uint64_t addr_base = UINT64_MAX;
        uint64_t rnglists_base = UINT64_MAX;

        uint64_t offset = 0;
        std::vector<StackEntry> stack;

        bool eof() const { return offset == end_offset; }

        explicit UnitState(llvm::DWARFUnit * u);
    };

    const FormatSettings format_settings;
    size_t num_threads;

    /// Dictionary columns shared among all LowCardinality columns we produce.
    ColumnPtr tag_dict_column;
    ColumnPtr attr_name_dict_column;
    ColumnPtr attr_form_dict_column;

    std::exception_ptr background_exception = nullptr;
    std::atomic<int> is_stopped{0};
    size_t approx_bytes_read_for_chunk = 0;

    std::optional<ThreadPool> pool;
    std::mutex mutex;
    std::condition_variable deliver_chunk;
    std::condition_variable wake_up_threads;
    std::deque<UnitState> units_queue;
    std::deque<std::pair<Chunk, size_t>> delivery_queue;
    size_t units_in_progress = 0;

    std::optional<Elf> elf;
    PODArray<char> file_contents; // if we couldn't mmap it

    std::unique_ptr<llvm::DWARFContext> dwarf_context;
    std::optional<llvm::DWARFDataExtractor> extractor; // .debug_info
    std::optional<llvm::DWARFDataExtractor> debug_line_extractor; // .debug_line
    std::optional<std::string_view> debug_addr_section; // .debug_addr
    std::optional<llvm::DWARFDataExtractor> debug_rnglists_extractor; // .debug_rnglists
    std::optional<llvm::DWARFDataExtractor> debug_ranges_extractor; // .debug_ranges

    std::atomic<size_t> seen_debug_line_warnings {0};

    void initializeIfNeeded();
    void initELF();
    void stopThreads();
    void parseFilenameTable(UnitState & unit, uint64_t offset);
    Chunk parseEntries(UnitState & unit);

    /// Parse .debug_addr entry.
    uint64_t fetchFromDebugAddr(uint64_t addr_base, uint64_t idx) const;
    /// Parse .debug_ranges (DWARF4) or .debug_rnglists (DWARF5) entry.
    void parseRanges(
        uint64_t offset, bool form_rnglistx, std::optional<uint64_t> low_pc, const UnitState & unit,
        const ColumnVector<UInt64>::MutablePtr & col_ranges_start,
        const ColumnVector<UInt64>::MutablePtr & col_ranges_end) const;
};

class DWARFSchemaReader : public ISchemaReader
{
public:
    DWARFSchemaReader(ReadBuffer & in_);

    NamesAndTypesList readSchema() override;
};

}

#endif
