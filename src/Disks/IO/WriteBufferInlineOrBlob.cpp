#include <Disks/IO/WriteBufferInlineOrBlob.h>

namespace DB
{

WriteBufferInlineOrBlob::WriteBufferInlineOrBlob(
    String file_name_,
    size_t max_inline_bytes_,
    CreateUnderlying create_underlying_,
    CommitInline commit_inline_,
    size_t buf_size)
    : WriteBufferFromFileBase(buf_size, nullptr, /*alignment=*/0)
    , file_name(std::move(file_name_))
    , max_inline_bytes(max_inline_bytes_)
    , create_underlying(std::move(create_underlying_))
    , commit_inline(std::move(commit_inline_))
{
}

void WriteBufferInlineOrBlob::spill()
{
    underlying = create_underlying();
    if (!accumulated.empty())
        underlying->write(accumulated.data(), accumulated.size());
    accumulated.clear();
}

void WriteBufferInlineOrBlob::nextImpl()
{
    size_t bytes = offset();
    if (!underlying && accumulated.size() + bytes > max_inline_bytes)
        spill();

    if (underlying)
        underlying->write(working_buffer.begin(), bytes);
    else
        accumulated.append(working_buffer.begin(), bytes);
}

void WriteBufferInlineOrBlob::preFinalize()
{
    next();
    if (underlying)
        underlying->preFinalize();
}

void WriteBufferInlineOrBlob::finalizeImpl()
{
    next();
    if (underlying)
        underlying->finalize();
    else
        commit_inline(std::move(accumulated));
}

void WriteBufferInlineOrBlob::cancelImpl() noexcept
{
    if (underlying)
        underlying->cancel();
}

void WriteBufferInlineOrBlob::sync()
{
    next();
    if (underlying)
        underlying->sync();
}

}
