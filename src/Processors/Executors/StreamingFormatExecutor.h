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

    StreamingFormatExecutor(
        const Block & header_,
        InputFormatPtr format_,
        ErrorCallback on_error_ = [](const MutableColumns &, const ColumnCheckpoints, Exception & e) -> size_t { throw std::move(e); },
        size_t total_bytes_ = 0,
        SimpleTransformPtr adding_defaults_transform_ = nullptr);

    /// Returns numbers of new read rows.
    size_t execute();

    /// Execute with provided read buffer.
    size_t execute(ReadBuffer & buffer, size_t num_bytes = 0);

    /// Inserts into result columns already preprocessed chunk.
    size_t insertChunk(Chunk chunk);

    /// Releases currently accumulated columns.
    MutableColumns getResultColumns();

    /// Sets query parameters for input format if applicable.
    void setQueryParameters(const NameToNameMap & parameters);

    /// Determines a conservative estimate of the average field size (8 bytes)
    /// assuming fields are generally larger than this value.
    static const size_t AVG_FIELD_SIZE_FOR_PREALLOCATE_PREDICTION = 8;

    // /// Vague estimate number of rows based on buffer size.
    // /// Helps guide preallocation to minimize reallocations
    // static size_t predictNumRows(
    //     size_t buffer_size,
    //     const Block & header,
    //     size_t max_rows = std::numeric_limits<uint64_t>::max());

private:
    void reserveResultColumns(size_t num_bytes);

    const Block header;
    const InputFormatPtr format;
    const ErrorCallback on_error;
    const SimpleTransformPtr adding_defaults_transform;

    InputPort port;
    MutableColumns result_columns;
    ColumnCheckpoints checkpoints;

    size_t total_bytes;
    bool try_reserve = true;
};

}
