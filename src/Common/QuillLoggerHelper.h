#pragma once

#include <quill/core/LogLevel.h>

namespace DB
{

quill::LogLevel parseQuillLogLevel(std::string_view level);

}
