#pragma once

#include <Processors/Formats/IInputFormat.h>

namespace DB
{

class StreamingFormatExecutor
{
public:
    using ErrorCallback = std::function<size_t(const MutableColumns &, Exception &)>;

    StreamingFormatExecutor(
        const Block & header_,
        InputFormatPtr format_,
        ErrorCallback on_error_ = [](const MutableColumns &, Exception &) -> size_t { throw; });

    size_t execute();
    MutableColumns getResultColumns();

private:
    Block header;
    InputFormatPtr format;
    ErrorCallback on_error;
    InputPort port;

    MutableColumns result_columns;
};

}
