#pragma once

namespace quill
{
inline namespace v8
{
class FrontendOptions;

template <typename TFrontendOptions>
class LoggerImpl;
}
}

using LoggerPtr =  quill::v8::LoggerImpl<quill::v8::FrontendOptions> *;
