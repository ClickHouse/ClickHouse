#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Processors/Formats/Impl/ValuesBlockInputFormat.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_EXCEPTION;
}

StreamingFormatExecutor::StreamingFormatExecutor(
    const Block & header_,
    InputFormatPtr format_,
    ErrorCallback on_error_,
    SimpleTransformPtr adding_defaults_transform_)
    : header(header_)
    , format(std::move(format_))
    , on_error(std::move(on_error_))
    , adding_defaults_transform(std::move(adding_defaults_transform_))
    , port(format->getPort().getHeader(), format.get())
    , result_columns(header.cloneEmptyColumns())
    , checkpoints(result_columns.size())
{
    connect(format->getPort(), port);

    for (size_t i = 0; i < result_columns.size(); ++i)
        checkpoints[i] = result_columns[i]->getCheckpoint();
}

MutableColumns StreamingFormatExecutor::getResultColumns()
{
    auto ret_columns = header.cloneEmptyColumns();
    std::swap(ret_columns, result_columns);
    return ret_columns;
}

void StreamingFormatExecutor::setQueryParameters(const NameToNameMap & parameters)
{
    /// Query parameters make sense only for format Values.
    if (auto * values_format = typeid_cast<ValuesBlockInputFormat *>(format.get()))
        values_format->setQueryParameters(parameters);
}

size_t StreamingFormatExecutor::execute(ReadBuffer & buffer)
{
    format->setReadBuffer(buffer);

    /// Format destructor can touch read buffer (for example when we use PeekableReadBuffer),
    /// but we cannot control lifetime of provided read buffer. To avoid heap use after free
    /// we call format->resetReadBuffer() method that resets all buffers inside format.
    SCOPE_EXIT(format->resetReadBuffer());
    return execute();
}

size_t StreamingFormatExecutor::execute()
{
    for (size_t i = 0; i < result_columns.size(); ++i)
        result_columns[i]->updateCheckpoint(*checkpoints[i]);

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
                    format->work();
                    break;

                case IProcessor::Status::Finished:
                    format->resetParser();
                    return new_rows;

                case IProcessor::Status::PortFull:
                    new_rows += insertChunk(port.pull());
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
        result_columns[i]->insertRangeFrom(*columns[i], 0, columns[i]->size());

    return chunk_rows;
}

}
