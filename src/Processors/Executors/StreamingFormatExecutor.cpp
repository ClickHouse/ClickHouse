#include <Processors/Executors/StreamingFormatExecutor.h>
#include <iostream>

namespace DB
{

StreamingFormatExecutor::StreamingFormatExecutor(
    const Block & header_, InputFormatPtr format_, ErrorCallback on_error_)
    : header(header_)
    , format(std::move(format_))
    , on_error(std::move(on_error_))
    , port(format->getPort().getHeader(), format.get())
{
    connect(format->getPort(), port);
    result_columns = header.cloneEmptyColumns();
}

MutableColumns StreamingFormatExecutor::getResultColumns()
{
    auto ret_columns = header.cloneEmptyColumns();
    std::swap(ret_columns, result_columns);
    return ret_columns;
}

size_t StreamingFormatExecutor::execute()
{
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
                {
                    auto chunk = port.pull();

                    auto chunk_rows = chunk.getNumRows();
                    new_rows += chunk_rows;

                    auto columns = chunk.detachColumns();

                    for (size_t i = 0, s = columns.size(); i < s; ++i)
                        result_columns[i]->insertRangeFrom(*columns[i], 0, columns[i]->size());

                    break;
                }
                case IProcessor::Status::NeedData:
                case IProcessor::Status::Async:
                case IProcessor::Status::ExpandPipeline:
                    throw Exception("Source processor returned status " + IProcessor::statusToName(status), ErrorCodes::LOGICAL_ERROR);
            }
        }
    }
    catch (Exception & e)
    {
        format->resetParser();
        return on_error(result_columns, e);
    }
}

}
