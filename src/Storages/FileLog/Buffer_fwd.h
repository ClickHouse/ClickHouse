#pragma once

#include <memory>

namespace DB
{
class FileLogConsumer;

using ReadBufferFromFileLogPtr = std::shared_ptr<FileLogConsumer>;
}
