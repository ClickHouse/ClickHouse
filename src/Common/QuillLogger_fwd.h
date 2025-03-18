#pragma once

namespace quill
{
inline namespace v8
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

using QuillLoggerPtr = quill::v8::LoggerImpl<QuillFrontendOptions> *;

using QuillFrontend = quill::FrontendImpl<QuillFrontendOptions>;
}
