#pragma once
#include "config.h"

#if USE_ODPS_TUNNEL && USE_ODPS_ARROW

#include <Storages/OdpsArrowReader.h>
#include <Storages/MaxComputeReadSession.h>
#include <Core/Block.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <Processors/ISource.h>

#include <memory>
#include <string>
#include <vector>

namespace DB
{

struct FormatSettings;
class ArrowColumnToCHColumn;

/// Validates requested column mappings against the download session's ODPS schema.
/// Unsupported mappings and inconsistent metadata raise exceptions; no reader fallback is performed.
class OdpsArrowColumnMatcher
{
public:
    static void validate(
        const apsara::odps::sdk::IODPSTableSchema * odps_schema,
        const std::vector<std::string> & odps_read_cols,
        const Block & sample_block);
};

/// Splits ODPS MAP columns of an Arrow record batch into a pair of
/// list columns - `col` of `map(K, V)` becomes `col.key` of `list(K)` plus
/// `col.value` of `list(V)` - BEFORE the batch reaches
/// `ArrowColumnToCHColumn`.
///
/// The split must run before conversion: the converter matches columns by
/// name, the header exposes the Nested names `col.key` / `col.value`, and its
/// own nested-extraction path cannot unwrap an Arrow MAP into them (its MAP
/// branch yields a ClickHouse `Map`, which `Nested::flatten` does not expand
/// into subcolumns). Splitting at the RecordBatch level makes the two list
/// columns ordinary name-matched columns, and the converter's LIST branch
/// plus its built-in `castColumn` produce exactly the header types
/// `Array(Nullable(K))` / `Array(Nullable(V))`.
///
/// The two list arrays share the map's offsets buffer and null bitmap, so the
/// split is zero-copy. The plan (which top-level names to split) is computed
/// once at construction from the header.
class OdpsArrowBatchPreprocessor
{
public:
    explicit OdpsArrowBatchPreprocessor(const Block & sample_block_);

    /// Returns `batch` unchanged when the plan is empty (zero-cost fast
    /// path); otherwise a reassembled batch with each planned MAP column
    /// replaced by its `.key` / `.value` pair. Throws on a planned column
    /// that is not an Arrow MAP (schema/header disagreement).
    std::shared_ptr<arrow::RecordBatch> process(const std::shared_ptr<arrow::RecordBatch> & batch) const;

private:
    /// ODPS top-level column names to split, deduplicated and in header order.
    std::vector<std::string> map_columns_to_split;
};

/// Columnar counterpart of `MaxComputeSource`: pulls whole Arrow record
/// batches from the ODPS tunnel and converts each batch into one chunk
/// through `ArrowColumnToCHColumn`. One batch per chunk keeps the server-side
/// batch row limit aligned with `max_block_size` (the server may still return
/// smaller batches when the byte limit applies).
class MaxComputeArrowSource : public ISource
{
public:
    String getName() const override { return "MaxComputeArrow"; }

    /// Uses a reloaded handle borrowed from `session_`; session completion is
    /// coordinated only after every source reports its exact row count.
    MaxComputeArrowSource(
        MaxComputeReadSessionPtr session_,
        const String & logger_name_,
        UInt64 max_block_size_,
        UInt64 count_,
        UInt64 start_,
        const Block & sample_block_,
        const std::vector<std::string> & odps_read_cols_,
        bool compress_,
        UInt64 max_batch_bytes_,
        UInt64 max_retries_,
        UInt64 retry_initial_backoff_ms_,
        UInt64 retry_max_backoff_ms_,
        UInt64 retry_max_elapsed_ms_,
        const FormatSettings & format_settings_);

    ~MaxComputeArrowSource() override;

protected:
    Chunk generate() override;

private:
    void finishTask();

    Poco::Logger * log;

    MaxComputeReadSessionPtr session;
    std::unique_ptr<AutoReconnectArrowReader> reader;
    std::unique_ptr<OdpsArrowBatchPreprocessor> preprocessor;
    std::unique_ptr<ArrowColumnToCHColumn> converter;
    bool eof = false;
    bool task_finished = false;
    Stopwatch watch;

    UInt64 count;
    Block sample_block;
};

}

#endif
