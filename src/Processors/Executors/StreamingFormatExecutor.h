#pragma once

#include <Processors/Formats/IInputFormat.h>
#include <Processors/ISimpleTransform.h>
#include <IO/EmptyReadBuffer.h>

namespace DB
{

using SimpleTransformPtr = std::shared_ptr<ISimpleTransform>;

/// Receives format and allows to execute
/// it multiple times for streaming processing of data.
class StreamingFormatExecutor
{
public:
    /// Callback is called, when exception is thrown in `execute` method.
    /// It provides currently accumulated columns to make a rollback, for example,
    /// and exception to rethrow it or add context to it.
    /// Should return number of new rows, which are added in callback
    /// to result columns in comparison to previous call of `execute`.
    using ErrorCallback = std::function<size_t(const MutableColumns &, const ColumnCheckpoints &, Exception &)>;

    /// Optional predicate polled once per chunk inside `execute`. When it returns true the
    /// execution is aborted with QUERY_WAS_CANCELLED (rethrown past `on_error`, not treated as a
    /// per-input parse error). Lets manually-driven callers honor query cancellation, which the
    /// pipeline executor would otherwise provide.
    using CancelCallback = std::function<bool()>;

    StreamingFormatExecutor(
        const Block & header_,
        InputFormatPtr format_,
        ErrorCallback on_error_ = [](const MutableColumns &, const ColumnCheckpoints, Exception & e) -> size_t { throw std::move(e); },
        size_t total_bytes_ = 0,
        size_t total_chunks_ = 0,
        SimpleTransformPtr adding_defaults_transform_ = nullptr,
        CancelCallback is_cancelled_ = {});

    /// Returns numbers of new read rows.
    size_t execute(size_t num_bytes = 0);

    /// Execute with provided read buffer.
    size_t execute(ReadBuffer & buffer, size_t num_bytes = 0);

    /// Inserts into result columns already preprocessed chunk.
    size_t insertChunk(Chunk chunk, size_t num_bytes = 0);

    /// Releases currently accumulated columns.
    MutableColumns getResultColumns();

    /// Sets query parameters for input format if applicable.
    void setQueryParameters(const NameToNameMap & parameters);

private:
    void preallocateResultColumns(size_t num_bytes, const Chunk & chunk);

    const Block header;
    const InputFormatPtr format;
    const ErrorCallback on_error;
    const SimpleTransformPtr adding_defaults_transform;
    const CancelCallback is_cancelled;

    InputPort port;
    MutableColumns result_columns;
    ColumnCheckpoints checkpoints;

    size_t total_bytes;
    size_t total_chunks;
    bool try_preallocate = true;
};

}
