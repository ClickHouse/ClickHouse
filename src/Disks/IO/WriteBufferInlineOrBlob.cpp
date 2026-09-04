#include <Disks/IO/WriteBufferInlineOrBlob.h>

#include <Core/Defines.h>

namespace DB
{

WriteBufferInlineOrBlob::WriteBufferInlineOrBlob(
    String file_name_,
    size_t max_inline_bytes_,
    bool create_blob_if_empty_,
    CreateUnderlying create_underlying_,
    FinalizeCallback finalize_callback_,
    size_t buf_size)
    /// This is a staging buffer only: before the spill it has to hold just the inline-or-blob
    /// decision window (`max_inline_bytes`), after it the data is copied into `underlying`, which
    /// does its own batching with a properly sized buffer. Allocating the full `buf_size` here
    /// would defeat the adaptive write buffer sizing of wide-part writers, where every column
    /// stream holds one of these buffers (see `use_adaptive_write_buffer`).
    : WriteBufferFromFileBase(
          std::min<size_t>(buf_size, std::max<size_t>(max_inline_bytes_, DBMS_DEFAULT_INITIAL_ADAPTIVE_BUFFER_SIZE)), nullptr, /*alignment=*/0)
    , file_name(std::move(file_name_))
    , max_inline_bytes(max_inline_bytes_)
    , create_blob_if_empty(create_blob_if_empty_)
    , create_underlying(std::move(create_underlying_))
    , finalize_callback(std::move(finalize_callback_))
{
    if (max_inline_bytes == 0)
        spill();
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
    if (underlying && (create_blob_if_empty || count() > 0))
        underlying->preFinalize();
}

void WriteBufferInlineOrBlob::finalizeImpl()
{
    next();

    if (!underlying)
    {
        finalize_callback(InlineData{std::move(accumulated)});
        return;
    }

    const size_t bytes_written = count();

    if (bytes_written == 0 && !create_blob_if_empty)
        underlying->cancel();
    else
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
