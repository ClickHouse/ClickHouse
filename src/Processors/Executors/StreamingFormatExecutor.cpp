#include <Columns/IColumn.h>
#include <Processors/Executors/StreamingFormatExecutor.h>

#include <base/scope_guard.h>
#include <Common/FailPoint.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_EXCEPTION;
    extern const int QUERY_WAS_CANCELLED;
}

namespace FailPoints
{
    extern const char async_insert_flush_pause_in_executor[];
}

StreamingFormatExecutor::StreamingFormatExecutor(
    const Block & header_,
    FormatCreator create_format_,
    ErrorCallback on_error_,
    size_t total_bytes_,
    size_t total_chunks_,
    TransformCreator create_adding_defaults_transform_,
    CancelCallback is_cancelled_)
    : header(header_)
    , create_format(std::move(create_format_))
    , on_error(std::move(on_error_))
    , create_adding_defaults_transform(std::move(create_adding_defaults_transform_))
    , is_cancelled(std::move(is_cancelled_))
    , result_columns(header.cloneEmptyColumns())
    , checkpoints(result_columns.size())
    , total_bytes(total_bytes_)
    , total_chunks(total_chunks_)
{
    createFormat();

    for (size_t i = 0; i < result_columns.size(); ++i)
        checkpoints[i] = result_columns[i]->getCheckpoint();
}

void StreamingFormatExecutor::createFormat()
{
    auto new_format = create_format();
    SimpleTransformPtr new_adding_defaults_transform;
    if (create_adding_defaults_transform)
        new_adding_defaults_transform = create_adding_defaults_transform(*new_format);
    port.emplace(new_format->getPort().getHeader(), new_format.get());
    connect(new_format->getPort(), *port);
    adding_defaults_transform = std::move(new_adding_defaults_transform);
    format = std::move(new_format);
}

void StreamingFormatExecutor::resetParser()
{
    if (format->supportsResetParser())
        format->resetParser();
}

MutableColumns StreamingFormatExecutor::getResultColumns()
{
    auto ret_columns = header.cloneEmptyColumns();
    std::swap(ret_columns, result_columns);
    return ret_columns;
}

void StreamingFormatExecutor::setQueryParameters(const NameToNameMap & parameters)
{
    if (!format)
        createFormat();
    format->setQueryParameters(parameters);
}

void StreamingFormatExecutor::preallocateResultColumns(size_t num_bytes, const Chunk & chunk)
{
    if (!try_preallocate)
        return;

    try_preallocate = false; /// do it once

    if (total_bytes && num_bytes && total_chunks > 1)
    {
        const auto & reference_columns = chunk.getColumns();
        size_t factor = static_cast<size_t>(std::ceil(static_cast<double>(total_bytes) / static_cast<double>(num_bytes)));

        /// assuming that all chunks have the same nature, specifically
        /// similar raw data size/number of rows ratio,
        ///  use first one to predict
        for (size_t i = 0; i < result_columns.size(); ++i)
        {
            /// prepareForSquashing is used to reserve space
            ///   for complex objects (string, array) we care about internal storages
            /// we don actually do squashing
            result_columns[i]->prepareForSquashing({reference_columns[i]}, factor);
        }
    }
}

size_t StreamingFormatExecutor::execute(ReadBuffer & buffer, size_t num_bytes)
{
    if (!format)
        createFormat();

    format->setReadBuffer(buffer);

    /// Format destructor can touch read buffer (for example when we use PeekableReadBuffer),
    /// but we cannot control lifetime of provided read buffer. To avoid heap use after free
    /// we call format->resetReadBuffer() method that resets all buffers inside format.
    SCOPE_EXIT({
        format->resetReadBuffer();
        if (!format->supportsResetParser())
        {
            port.reset();
            adding_defaults_transform.reset();
            format.reset();
        }
    });
    return execute(num_bytes);
}

size_t StreamingFormatExecutor::execute(size_t num_bytes)
{
    for (size_t i = 0; i < result_columns.size(); ++i)
        result_columns[i]->updateCheckpoint(*checkpoints[i]);

    try
    {
        size_t new_rows = 0;
        port->setNeeded();
        while (true)
        {
            FailPointInjection::pauseFailPoint(FailPoints::async_insert_flush_pause_in_executor);

            if (is_cancelled && is_cancelled())
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Format streaming was cancelled");

            auto status = format->prepare();

            switch (status)
            {
                case IProcessor::Status::Ready:
                    format->work();
                    break;

                case IProcessor::Status::Finished:
                    resetParser();
                    return new_rows;

                case IProcessor::Status::PortFull:
                    new_rows += insertChunk(port->pull(), num_bytes);
                    break;

                case IProcessor::Status::NeedData:
                case IProcessor::Status::Async:
                case IProcessor::Status::UpdatePipeline:
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Source processor returned status {}", IProcessor::statusToName(status));
            }
        }
    }
    catch (Exception & e)
    {
        resetParser();
        /// Cancellation aborts the whole execution; it is not a recoverable per-input parse error.
        /// A cancelled `QueryStatus` reports an arbitrary exception, so ask the callback rather than
        /// match on the code, which for a callback-less caller `on_error` must keep handling.
        if (e.code() == ErrorCodes::QUERY_WAS_CANCELLED || (is_cancelled && is_cancelled()))
            throw;
        return on_error(result_columns, checkpoints, e);
    }
    catch (std::exception & e)
    {
        resetParser();
        auto exception = Exception(Exception::CreateFromSTDTag{}, e);
        return on_error(result_columns, checkpoints, exception);
    }
    catch (...) // Ok: wrap unknown exception and pass to on_error callback
    {
        resetParser();
        auto exception = Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Unknown exception while executing StreamingFormatExecutor with format {}", format->getName());
        return on_error(result_columns, checkpoints, exception);
    }
}

size_t StreamingFormatExecutor::insertChunk(Chunk chunk, size_t num_bytes)
{
    size_t chunk_rows = chunk.getNumRows();
    if (adding_defaults_transform)
        adding_defaults_transform->transform(chunk);

    preallocateResultColumns(num_bytes, chunk);

    auto columns = chunk.detachColumns();
    for (size_t i = 0, s = columns.size(); i < s; ++i)
        result_columns[i]->insertRangeFrom(*columns[i], 0, columns[i]->size());

    return chunk_rows;
}

}
