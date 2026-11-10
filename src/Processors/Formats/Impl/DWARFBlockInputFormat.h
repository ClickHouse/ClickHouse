#pragma once
#include "config.h"
#if USE_DWARF_PARSER && defined(__ELF__) && !defined(OS_FREEBSD)

#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <Common/PODArray.h>
#include <IO/MMappedFile.h>
#include <Processors/Chunk.h>

#include <dwarf_rust.h>

#include <Common/DequeWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include <array>
#include <atomic>
#include <condition_variable>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

namespace DB
{


/// The `DWARF` input format backed by the Rust parser (`contrib/dwarf-cmake`).
///
/// This class only owns the input bytes, the worker threads and the delivery queue, and it turns the
/// column buffers filled by Rust into ClickHouse columns. Parsing of the ELF container, the DWARF
/// sections, the DIEs, the line table and the range lists happens entirely in Rust.
///
/// Lifetime order (enforced by `releaseState`): stop and join the workers, free the unit handles,
/// free the file handle, and only then release the input bytes the handles point into.
class DWARFBlockInputFormat final : public IInputFormat
{
public:
    DWARFBlockInputFormat(ReadBuffer & in_, SharedHeader header_, const FormatSettings & format_settings_, size_t num_threads_);
    ~DWARFBlockInputFormat() override;

    String getName() const override { return "DWARFBlockInputFormat"; }

    void resetParser() override;

    size_t getApproxBytesReadForChunk() const override { return approx_bytes_read_for_chunk; }

protected:
    Chunk read() override;

    void onCancel() noexcept override;

private:
    struct FileDeleter
    {
        void operator()(ChDwarfFile * handle) const noexcept { ch_dwarf_file_free(handle); }
    };

    struct UnitDeleter
    {
        void operator()(ChDwarfUnit * handle) const noexcept { ch_dwarf_unit_free(handle); }
    };

    using FilePtr = std::unique_ptr<ChDwarfFile, FileDeleter>;
    using UnitPtr = std::unique_ptr<ChDwarfUnit, UnitDeleter>;

    /// A unit handle migrates between worker threads between calls, but is never used concurrently.
    struct UnitState
    {
        size_t unit_index = 0;
        UnitPtr handle;
        UInt64 unit_offset = 0;
        /// Copied out of Rust after every chunk, because Rust only keeps it valid until the next call.
        std::string unit_name;
    };

    struct ChunkResult
    {
        Chunk chunk;
        /// `.debug_info` bytes advanced by this chunk.
        size_t bytes_advanced = 0;
        /// No more chunks will be produced for this unit (it is complete or the read was cancelled).
        bool finished = false;
    };

    const FormatSettings format_settings;
    size_t num_threads;

    /// Dictionary columns shared among all LowCardinality columns we produce.
    ColumnPtr tag_dict_column;
    ColumnPtr attr_name_dict_column;
    ColumnPtr attr_form_dict_column;

    /// For each column of the requested header, its position in the fixed DWARF schema.
    VectorWithMemoryTracking<size_t> output_columns;
    /// Which of the fixed-schema columns were requested.
    std::array<bool, 16> need{};
    ChDwarfProjection projection{};
    /// Which Rust output sinks the projection needs.
    std::array<bool, CH_DWARF_SINK_COUNT> sink_active{};

    std::exception_ptr background_exception = nullptr;
    std::atomic<int> is_stopped{0};
    size_t approx_bytes_read_for_chunk = 0;

    std::optional<ThreadPoolCallbackRunnerLocal<void>> runner;
    std::mutex mutex;
    std::condition_variable deliver_chunk;
    std::condition_variable wake_up_threads;
    DequeWithMemoryTracking<UnitState> units_queue;
    DequeWithMemoryTracking<std::pair<Chunk, size_t>> delivery_queue;
    size_t units_in_progress = 0;

    bool initialized = false;

    /// The immutable input bytes. Either a dedicated read-only mapping of a regular local file,
    /// or a full copy of the input. Neither is resized or remapped while `file` is alive.
    std::optional<MMappedFile> mapped_file;
    PODArray<char> file_contents;
    const uint8_t * input_data = nullptr;
    size_t input_size = 0;

    /// Declared after the input bytes so that, even without `releaseState`, it is destroyed first.
    FilePtr file;

    void initializeIfNeeded();
    /// Maps or copies the input. Returns false if the read was cancelled while copying.
    bool loadInput();
    void stopThreads();
    /// Frees everything that depends on the input bytes, in the required order, and the bytes themselves.
    /// The workers must have been stopped and joined.
    void releaseState();
    ChunkResult parseChunk(UnitState & unit);
};


class DWARFSchemaReader final : public ISchemaReader
{
public:
    explicit DWARFSchemaReader(ReadBuffer & in_);

    NamesAndTypesList readSchema() override;
};

}

#endif
