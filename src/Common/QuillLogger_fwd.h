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

using QuillLoggerPtr = quill::LoggerImpl<QuillFrontendOptions> *;

using QuillFrontend = quill::FrontendImpl<QuillFrontendOptions>;
}
