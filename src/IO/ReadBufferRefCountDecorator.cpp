#include <IO/ReadBufferRefCountDecorator.h>
#include <fmt/format.h>
#include "Common/logger_useful.h"

namespace DB
{

DB::ReadBufferRefCountDecorator::ReadBufferRefCountDecorator(ReadBuffer & impl_)
    : ReadBuffer(nullptr, 0, 0)
    , impl(impl_)
{
    logger = getLogger(fmt::format("ReadBufferRefCountDecorator {}", size_t(this)));
    // proxy position and size from underlying buffer to this buffer
    working_buffer = Buffer(impl.position(), impl.buffer().end());
    position() = working_buffer.begin();
    LOG_DEBUG(logger, "c-tor & avalable size: {} offset {} position {} begin {} end {}", available(), offset(), size_t(position()), size_t(working_buffer.begin()), size_t(working_buffer.end()));
}

DB::ReadBufferRefCountDecorator::ReadBufferRefCountDecorator(std::unique_ptr<ReadBuffer> impl_)
    : ReadBuffer(nullptr, 0, 0)
    , impl(*impl_)
    , holder(std::move(impl_))
{
    logger = getLogger(fmt::format("ReadBufferRefCountDecorator {}", size_t(this)));
    // proxy position and size from underlying buffer to this buffer
    working_buffer = Buffer(impl.position(), impl.buffer().end());
    position() = working_buffer.begin();
    LOG_DEBUG(logger, "c-tor unuque avalable size: {} offset {} position {} begin {} end {}", available(), offset(), size_t(position()), size_t(working_buffer.begin()), size_t(working_buffer.end()));
}

size_t DB::ReadBufferRefCountDecorator::getRefCount()
{
    return shared_from_this().use_count() - 1;
}

bool DB::ReadBufferRefCountDecorator::nextImpl()
{
    LOG_DEBUG(logger, "nextImpl avalable size: {} offset {} position {} begin {} end {}", available(), offset(), size_t(position()), size_t(buffer().begin()), size_t(buffer().end()));
    LOG_DEBUG(logger, "nextImpl impl ::: avalable size: {} offset {} position {} begin {} end {}", impl.available(), impl.offset(), size_t(impl.position()), size_t(impl.buffer().begin()), size_t(impl.buffer().end()));
    // sync progress with underlying buffer
    impl.position() = position();
    LOG_DEBUG(logger, "nextImpl impl avalable size aftery sync: {} offset {} position {} begin {} end {}", impl.available(), impl.offset(), size_t(impl.position()), size_t(impl.buffer().begin()), size_t(impl.buffer().end()));
    // read next chunk from underlying buffer
    bool result = impl.next();
    // proxy position and size from underlying buffer to this buffer
    working_buffer = Buffer(impl.position(), impl.buffer().end());
    position() = working_buffer.begin();
    LOG_DEBUG(logger, "nextImpl res {} avalable size: {} offset {} position {} begin {} end {}", result, available(), offset(), size_t(position()), size_t(working_buffer.begin()), size_t(working_buffer.end()));

    return result;
}

ReadBufferRefCountDecorator::~ReadBufferRefCountDecorator()
{
    LOG_DEBUG(logger, "d-tor avalable size: {} offset {} position {} begin {} end {}", available(), offset(), size_t(position()), size_t(working_buffer.begin()), size_t(working_buffer.end()));
    impl.position() = position();
}
}
