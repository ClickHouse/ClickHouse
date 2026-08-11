#pragma once

#include "config.h"

#if USE_VORTEX

#include <Core/BlockMissingValues.h>
#include <Formats/FormatFilterInfo.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>

namespace arrow { class Schema; }
namespace arrow::io { class RandomAccessFile; }

struct VortexFFIReader;
struct VortexFFIScanner;

namespace DB
{

class ArrowColumnToCHColumn;
struct VortexReadContext;

/// Reads Vortex files (https://github.com/vortex-data/vortex, https://docs.vortex.dev/) through
/// the Rust `vortex` library, see `rust/workspace/vortex`. The library reads the file through a
/// callback backed by a seekable ClickHouse read buffer (or by an in-memory copy of the file if
/// the buffer is not seekable), and returns decoded chunks over the Arrow C Data Interface,
/// which are then converted to ClickHouse columns the same way as in the Arrow format.
class VortexBlockInputFormat final : public IInputFormat
{
public:
    VortexBlockInputFormat(
        ReadBuffer & in_,
        SharedHeader header_,
        const FormatSettings & format_settings_,
        FormatFilterInfoPtr format_filter_info_);
    ~VortexBlockInputFormat() override;

    String getName() const override { return "VortexBlockInputFormat"; }

    void resetParser() override;

    const BlockMissingValues * getMissingValues() const override;

    size_t getApproxBytesReadForChunk() const override { return approx_bytes_read_for_chunk; }

private:
    Chunk read() override;

    void onCancel() noexcept override { is_stopped = 1; }

    void prepareReader();
    void closeReader();

    /// Produces chunks for queries that need no columns from the file (e.g. `SELECT count()`),
    /// where only the number of rows matters.
    Chunk readWithoutColumns();

    std::shared_ptr<arrow::io::RandomAccessFile> arrow_file;
    std::unique_ptr<VortexReadContext> read_context;
    VortexFFIReader * reader = nullptr;
    VortexFFIScanner * scanner = nullptr;
    std::shared_ptr<arrow::Schema> file_schema;

    std::unique_ptr<ArrowColumnToCHColumn> arrow_column_to_ch_column;

    /// The number of rows left to return for queries that read no columns from the file.
    UInt64 pending_rows_without_columns = 0;
    bool count_returned = false;

    BlockMissingValues block_missing_values;
    size_t approx_bytes_read_for_chunk = 0;
    size_t previous_approx_bytes_read = 0;

    const FormatSettings format_settings;
    FormatFilterInfoPtr format_filter_info;

    std::atomic<int> is_stopped{0};
};

class VortexSchemaReader final : public ISchemaReader
{
public:
    VortexSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_);
    ~VortexSchemaReader() override;

    NamesAndTypesList readSchema() override;

    std::optional<size_t> readNumberOrRows() override;

private:
    void initializeIfNeeded();

    const FormatSettings format_settings;

    std::shared_ptr<arrow::io::RandomAccessFile> arrow_file;
    std::unique_ptr<VortexReadContext> read_context;
    VortexFFIReader * reader = nullptr;
    std::shared_ptr<arrow::Schema> file_schema;

    /// Never set; the file wrapper created by asArrowFile keeps a reference to it.
    std::atomic<int> is_stopped{0};
};

}

#endif
