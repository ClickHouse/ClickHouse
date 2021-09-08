#pragma once

#include <Processors/Formats/IInputFormat.h>
#include <Processors/ISimpleTransform.h>

namespace DB
{

using SimpleTransformPtr = std::shared_ptr<ISimpleTransform>;

class StreamingFormatExecutor
{
public:
    using ErrorCallback = std::function<size_t(const MutableColumns &, Exception &)>;

    StreamingFormatExecutor(
        const Block & header_,
        InputFormatPtr format_,
        ErrorCallback on_error_ = [](const MutableColumns &, Exception &) -> size_t { throw; },
        SimpleTransformPtr adding_defaults_transform_ = nullptr);

    size_t execute();
    MutableColumns getResultColumns();

private:
    const Block header;
    const InputFormatPtr format;
    const ErrorCallback on_error;
    const SimpleTransformPtr adding_defaults_transform;

    InputPort port;
    MutableColumns result_columns;
};

}
