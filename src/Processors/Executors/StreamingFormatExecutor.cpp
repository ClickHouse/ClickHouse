#include <exception>
#include <memory>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Processors/Formats/IInputFormatErrorsHandler.h>
#include <Processors/Formats/IRowInputFormat.h>

namespace
{

using namespace DB;

class InputFormatErrorsProxy : public IInputFormatErrorsHandler
{
public:
    explicit InputFormatErrorsProxy(const StreamingFormatExecutor::ErrorCallback & on_error_)
        : on_error(on_error_)
    {}

    void logError(ErrorEntry) override
    {
        on_error(std::current_exception());
    }

private:
    const StreamingFormatExecutor::ErrorCallback & on_error;
};

}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
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
{
    connect(format->getPort(), port);
    format->setErrorsHandler(std::make_shared<InputFormatErrorsProxy>(on_error));
}
StreamingFormatExecutor::~StreamingFormatExecutor()
{
    format->setErrorsHandler({});
}

MutableColumns StreamingFormatExecutor::getResultColumns()
{
    auto ret_columns = header.cloneEmptyColumns();
    std::swap(ret_columns, result_columns);
    return ret_columns;
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
    size_t new_rows = 0;

    try
    {
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
    /// All IRowInputFormat formats handled via InputFormatErrorsProxy, this
    /// catch handler only for non row-based formats.
    catch (const Exception & e)
    {
        if (!isParseError(e.code()))
            throw;

        format->resetParser();
        on_error(std::current_exception());
        return new_rows;
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
