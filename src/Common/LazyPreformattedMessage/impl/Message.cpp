#include <Common/LazyPreformattedMessage/impl/Message.h>
#include <Common/LazyPreformattedMessage/impl/Storage.h>
#include <base/defines.h>

#include <utility>

namespace DB::LazyPreformattedMessageImpl
{

Message::Message(fmt::string_view fmt_str_, FormatFn format_fn_, DestroyFn destroy_fn_, void * args_, size_t lane_)
    : fmt_str(fmt_str_)
    , format_fn(format_fn_)
    , destroy_fn(destroy_fn_)
    , args(args_)
    , lane(lane_)
{
    chassert(format_fn);
    chassert(destroy_fn);
}

Message::Message(Message && other) noexcept
    : fmt_str(other.fmt_str)
    , format_fn(other.format_fn)
    , destroy_fn(other.destroy_fn)
    , args(std::exchange(other.args, nullptr))
    , lane(other.lane)
{
    chassert(format_fn);
    chassert(destroy_fn);
}

Message & Message::operator=(Message && other) noexcept
{
    if (this == &other)
        return *this;

    destroy();
    fmt_str = other.fmt_str;
    format_fn = other.format_fn;
    destroy_fn = other.destroy_fn;
    args = std::exchange(other.args, nullptr);
    lane = other.lane;
    return *this;
}

Message::~Message()
{
    destroy();
}

std::string Message::format() const
{
    return format_fn(fmt_str, args);
}

void Message::destroy() noexcept
{
    if (void * ptr = std::exchange(args, nullptr))
    {
        destroy_fn(ptr);
        Storage::deallocate(lane);
    }
}

}
