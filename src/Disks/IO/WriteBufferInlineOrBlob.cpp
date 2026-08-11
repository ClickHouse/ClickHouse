#include <Disks/IO/WriteBufferInlineOrBlob.h>

namespace DB
{

WriteBufferInlineOrBlob::WriteBufferInlineOrBlob(
    String file_name_,
    size_t max_inline_bytes_,
    bool create_blob_if_empty_,
    CreateUnderlying create_underlying_,
    FinalizeCallback finalize_callback_,
    size_t buf_size)
    : WriteBufferFromFileBase(buf_size, nullptr, /*alignment=*/0)
    , file_name(std::move(file_name_))
    , max_inline_bytes(max_inline_bytes_)
    , create_blob_if_empty(create_blob_if_empty_)
    , create_underlying(std::move(create_underlying_))
    , finalize_callback(std::move(finalize_callback_))
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

    if (!underlying && max_inline_bytes > 0)
    {
        finalize_callback(InlineData{std::move(accumulated)});
        return;
    }

    /// Inlining is disabled and nothing was written: upload the empty blob only when the
    /// metadata cannot represent an empty file without one.
    if (!underlying && create_blob_if_empty)
        spill();

    const size_t bytes_written = count();
    if (underlying)
        underlying->finalize();
    finalize_callback(WrittenBlob{bytes_written});
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
