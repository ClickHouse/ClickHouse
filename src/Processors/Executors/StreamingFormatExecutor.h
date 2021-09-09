#pragma once

#include <Processors/Formats/IInputFormat.h>
#include <Processors/ISimpleTransform.h>

namespace DB
{

using SimpleTransformPtr = std::shared_ptr<ISimpleTransform>;

/// Recieves format and allows to execute
/// it multiple times for streaming processing of data.
class StreamingFormatExecutor
{
public:
    /// Callback is called, when got exception, while executing format.
    /// It provides currently accumulated columns to make a roolback, for example,
    /// and exception to rethrow or add context to it.
    /// Should return number of rows, which are added to callback
    /// to result columns in comparison to previous call of `execute`.
    using ErrorCallback = std::function<size_t(const MutableColumns &, Exception &)>;

    StreamingFormatExecutor(
        const Block & header_,
        InputFormatPtr format_,
        ErrorCallback on_error_ = [](const MutableColumns &, Exception &) -> size_t { throw; },
        SimpleTransformPtr adding_defaults_transform_ = nullptr);

    /// Returns numbers of newly read rows.
    size_t execute();

    /// Releases currently accumulated columns.
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
