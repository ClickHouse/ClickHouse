#include <IO/ReadBufferRefCountDecorator.h>

namespace DB
{

DB::ReadBufferRefCountDecorator::ReadBufferRefCountDecorator(ReadBuffer & impl_)
    : ReadBuffer(nullptr, 0, 0)
    , impl(impl_)
{
    // proxy position and size from underlying buffer to this buffer
    BufferBase::set(impl.buffer().begin(), impl.buffer().size(), impl.offset());
}

DB::ReadBufferRefCountDecorator::ReadBufferRefCountDecorator(std::unique_ptr<ReadBuffer> impl_)
    : ReadBuffer(nullptr, 0, 0)
    , impl(*impl_)
    , holder(std::move(impl_))
{
    // proxy position and size from underlying buffer to this buffer
    BufferBase::set(impl.buffer().begin(), impl.buffer().size(), impl.offset());
}

size_t DB::ReadBufferRefCountDecorator::getRefCount()
{
    return shared_from_this().use_count() - 1;
}

bool DB::ReadBufferRefCountDecorator::nextImpl()
{
    // sync progress with underlying buffer
    impl.position() = position();
    // read next chunk from underlying buffer
    bool result = impl.next();
    // proxy position and size from underlying buffer to this buffer
    BufferBase::set(impl.buffer().begin(), impl.buffer().size(), impl.offset());
    return result;
}

}
