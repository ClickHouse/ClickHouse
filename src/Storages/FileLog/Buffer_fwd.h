#pragma once

#include <memory>

namespace DB
{
class ReadBufferFromFileLog;

using ReadBufferFromFileLogPtr = std::shared_ptr<ReadBufferFromFileLog>;
}
