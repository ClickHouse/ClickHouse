#pragma once


namespace quill
{
inline namespace v9
{

template <typename TFrontendOptions>
class LoggerImpl;

template <typename TFrontendOptions>
class FrontendImpl;

class Sink;
}
}

namespace DB
{
struct QuillFrontendOptions;

using QuillLogger = quill::LoggerImpl<QuillFrontendOptions>;
using QuillLoggerPtr = QuillLogger *;

using QuillFrontend = quill::FrontendImpl<QuillFrontendOptions>;
}
