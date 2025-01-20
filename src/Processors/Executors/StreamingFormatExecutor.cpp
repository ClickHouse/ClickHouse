#include <Core/ServerSettings.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Processors/Formats/Impl/ValuesBlockInputFormat.h>

#include <Common/logger_useful.h>

namespace DB
{

namespace ServerSetting
{
    extern const ServerSettingsUInt64 avg_field_size_for_preallocate_prediction;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_EXCEPTION;
}

StreamingFormatExecutor::StreamingFormatExecutor(
    const Block & header_,
    InputFormatPtr format_,
    ErrorCallback on_error_,
    size_t /* total_bytes_ */,
    SimpleTransformPtr adding_defaults_transform_)
    : header(header_)
    , format(std::move(format_))
    , on_error(std::move(on_error_))
    , adding_defaults_transform(std::move(adding_defaults_transform_))
    , port(format->getPort().getHeader(), format.get())
    , result_columns(header.cloneEmptyColumns())
    , checkpoints(result_columns.size())
    // , total_bytes(total_bytes_)
    , squashing(header_, std::numeric_limits<uint64_t>::max(), 0)
{
    // LOG_WARNING(
    //     &Poco::Logger::get("StreamingFormatExecutor"),
    //     "ctor");

    connect(format->getPort(), port);

    for (size_t i = 0; i < result_columns.size(); ++i)
        checkpoints[i] = result_columns[i]->getCheckpoint();

    // if (estimated_rows_)
    // {
    //     // std::lock_guard lock(prealloc_mutex);
    //     for (size_t i = 0; i < result_columns.size(); ++i)
    //         result_columns[i]->reserve(estimated_rows_);
    // }


    // LOG_WARNING(
    //     &Poco::Logger::get("StreamingFormatExecutor"),
    //     "reserved called {} times for {} rows", result_columns.size(), estimated_rows_);

    // for (size_t i = 0; i < result_columns.size(); ++i)
    // {
    //     LOG_WARNING(
    //         &Poco::Logger::get("StreamingFormatExecutor"),
    //         "addr {}", static_cast<const void*>(result_columns[i]->getRawData().begin()));
    // }


        // for (auto & column : result_columns)
        //     column->reserve(estimated_rows_);
}

MutableColumns StreamingFormatExecutor::getResultColumns()
{
    // auto ret_columns = header.cloneEmptyColumns();
    // std::swap(ret_columns, result_columns);
    // return ret_columns;

    // return squashing.flush().mutateColumns();

    ///// Chunk result_chunk = Squashing::squash(squashing.flush());
    return Squashing::squash(squashing.flush()).mutateColumns();

}

void StreamingFormatExecutor::setQueryParameters(const NameToNameMap & parameters)
{
    /// Query parameters make sense only for format Values.
    if (auto * values_format = typeid_cast<ValuesBlockInputFormat *>(format.get()))
        values_format->setQueryParameters(parameters);
}

// void StreamingFormatExecutor::reserveResultColumns(size_t num_bytes)
// {
//     if (!try_reserve)
//         return;

//     try_reserve = false;

//     if (total_bytes && num_bytes)
//     {
//         size_t ratio = total_bytes / num_bytes;
//         if (ratio > 4)
//         {
//             for (size_t i = 0; i < result_columns.size(); ++i)
//             {
//                 result_columns[i]->reserve(result_columns[i]->capacity() * ratio);
//             }
//         }
//     }
// }

size_t StreamingFormatExecutor::execute(ReadBuffer & buffer, size_t /* num_bytes */)
{
    format->setReadBuffer(buffer);

    /// Format destructor can touch read buffer (for example when we use PeekableReadBuffer),
    /// but we cannot control lifetime of provided read buffer. To avoid heap use after free
    /// we call format->resetReadBuffer() method that resets all buffers inside format.
    SCOPE_EXIT(format->resetReadBuffer());
    auto new_rows = execute();
    // reserveResultColumns(num_bytes);
    return new_rows;
}

size_t StreamingFormatExecutor::execute()
{
    for (size_t i = 0; i < result_columns.size(); ++i)
        result_columns[i]->updateCheckpoint(*checkpoints[i]);

    // LOG_WARNING(
    //     &Poco::Logger::get("StreamingFormatExecutor"),
    //     "execute");

    try
    {
        size_t new_rows = 0;
        port.setNeeded();
        while (true)
        {
            auto status = format->prepare();

            switch (status)
            {
                case IProcessor::Status::Ready:
                    // LOG_WARNING(
                    //     &Poco::Logger::get("StreamingFormatExecutor"),
                    //     "format->work()");
                    format->work();
                    break;

                case IProcessor::Status::Finished:
                    // LOG_WARNING(
                    //     &Poco::Logger::get("StreamingFormatExecutor"),
                    //     "resetParser");
                    format->resetParser();
                    return new_rows;

                case IProcessor::Status::PortFull:
                    {
                        // LOG_WARNING(
                        //     &Poco::Logger::get("StreamingFormatExecutor"),
                        //     "insertChunk");




                        // new_rows += insertChunk(port.pull());



                        auto chunk = port.pull();
                        new_rows += chunk.getNumRows();

                        squashing.add(std::move(chunk));
                    }

                    break;

                case IProcessor::Status::NeedData:
                case IProcessor::Status::Async:
                case IProcessor::Status::ExpandPipeline:
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Source processor returned status {}", IProcessor::statusToName(status));
            }
        }
    }
    catch (Exception & e)
    {
        format->resetParser();
        return on_error(result_columns, checkpoints, e);
    }
    catch (std::exception & e)
    {
        format->resetParser();
        auto exception = Exception(Exception::CreateFromSTDTag{}, e);
        return on_error(result_columns, checkpoints, exception);
    }
    catch (...)
    {
        format->resetParser();
        auto exception = Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Unknown exception while executing StreamingFormatExecutor with format {}", format->getName());
        return on_error(result_columns, checkpoints, exception);
    }
}

size_t StreamingFormatExecutor::insertChunk(Chunk chunk)
{
    size_t chunk_rows = chunk.getNumRows();
    if (adding_defaults_transform)
        adding_defaults_transform->transform(chunk);

    auto columns = chunk.detachColumns();
    for (size_t i = 0, s = columns.size(); i < s; ++i)
    {
        // LOG_WARNING(
        //     &Poco::Logger::get("StreamingFormatExecutor"),
        //     "addr before {}", static_cast<const void*>(result_columns[i]->getRawData().begin()));
        result_columns[i]->insertRangeFrom(*columns[i], 0, columns[i]->size());
        // LOG_WARNING(
        //     &Poco::Logger::get("StreamingFormatExecutor"),
        //     "addr after {}", static_cast<const void*>(result_columns[i]->getRawData().begin()));
    }


    return chunk_rows;
}

// size_t StreamingFormatExecutor::predictNumRows(size_t buffer_size, const Block & header, size_t max_rows)
// {
//     auto avg_field_size = Context::getGlobalContextInstance()->getServerSettings()[ServerSetting::avg_field_size_for_preallocate_prediction];
//     return avg_field_size ? std::min(buffer_size / ( header.columns() * avg_field_size), max_rows) : 0;
// }

}
