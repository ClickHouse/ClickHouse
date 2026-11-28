#include <Processors/Formats/Impl/Vortex/VortexFFIHelpers.h>

#if USE_VORTEX

#include <Formats/FormatSettings.h>
#include <IO/ReadBuffer.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/WithFileSize.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <Processors/Formats/Impl/ArrowIPC/ArrowCData.h>
#include <Processors/Formats/Impl/ArrowIPC/SchemaConverter.h>
#include <base/scope_guard.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>

#include <algorithm>

#include <vortex_ffi.h>

namespace ProfileEvents
{
extern const Event VortexReadRequests;
extern const Event VortexReadBytes;
}

namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_DATA;
extern const int CANNOT_READ_ALL_DATA;
}

}

namespace DB::Vortex
{

static constexpr auto VORTEX_MAGIC_BYTES = "VTXF";

void VortexExpressionDeleter::operator()(FFI_VortexExpression * expression) const
{
    vortex_ffi_expr_free(expression);
}

/// An exception escaping into Rust would unwind across the FFI boundary, so the callback catches
/// everything and reports failure with its return value.
extern "C" int32_t vortexFFIReadCallback(void * context, uint64_t offset, uint64_t length, uint8_t * out);
extern "C" int32_t vortexFFIReadCallback(void * context, uint64_t offset, uint64_t length, uint8_t * out)
{
    auto * ctx = static_cast<VortexReadContext *>(context);
    try
    {
        ctx->file->readAt(reinterpret_cast<char *>(out), length, offset);
        ctx->bytes_read.fetch_add(length, std::memory_order_relaxed);
        ProfileEvents::increment(ProfileEvents::VortexReadRequests);
        ProfileEvents::increment(ProfileEvents::VortexReadBytes, length);
        return 0;
    }
    catch (...)
    {
        ctx->setException(std::current_exception());
        return 1;
    }
}

std::unique_ptr<VortexFile> VortexFile::open(ReadBuffer & in, const FormatSettings & format_settings, std::atomic<int> & is_stopped)
{
    auto file = std::make_unique<VortexFile>();
    auto * seekable = dynamic_cast<SeekableReadBuffer *>(&in);
    if (seekable && isBufferWithFileSize(in) && format_settings.seekable_read)
    {
        /// Read-ahead would only fetch bytes the library never asked for: it chooses the byte ranges itself and
        /// merges the nearby ones.
        if (seekable->supportsReadAt())
        {
            file->seekable_in = seekable;
            file->read_at = true;
            file->file_size = getFileSizeFromReadBuffer(in);
            return file;
        }
        if (seekable->checkIfActuallySeekable())
        {
            file->seekable_in = seekable;
            file->file_size = getFileSizeFromReadBuffer(in);
            return file;
        }
    }

    /// Avoid loading the whole file if it doesn't seem to even be in the correct format.
    const std::string_view magic = VORTEX_MAGIC_BYTES;
    file->data.resize(magic.size());
    const size_t bytes_read = in.read(file->data.data(), magic.size());
    if (bytes_read < magic.size() || file->data != magic)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Not a Vortex file");
    {
        WriteBufferFromString file_buffer(file->data, AppendModeTag{});
        copyData(in, file_buffer, is_stopped);
    }
    file->file_size = file->data.size();
    return file;
}

void VortexFile::readAt(char * to, size_t length, size_t offset)
{
    if (offset > file_size || length > file_size - offset)
        throw Exception(
            ErrorCodes::CANNOT_READ_ALL_DATA,
            "Unexpected end of Vortex file: cannot read {} bytes at offset {} of a file of {} bytes",
            length, offset, file_size);

    if (!seekable_in)
    {
        memcpy(to, data.data() + offset, length);
        return;
    }

    size_t bytes_read = 0;
    if (read_at)
    {
        bytes_read = seekable_in->readBigAt(to, length, offset, nullptr);
    }
    else
    {
        std::lock_guard lock(mutex);
        /// Seeking past an earlier `setReadUntilPosition` confuses some of the buffers.
        seekable_in->setReadUntilEnd();
        seekable_in->seek(offset, SEEK_SET);
        seekable_in->setReadUntilPosition(offset + length);
        bytes_read = seekable_in->readBig(to, length);
    }
    if (bytes_read != length)
        throw Exception(
            ErrorCodes::CANNOT_READ_ALL_DATA,
            "Unexpected end of Vortex file: read {} bytes instead of {} at offset {}",
            bytes_read, length, offset);
}

String takeVortexError(char * error)
{
    String message = error ? String(error) : "unknown error";
    if (error)
        vortex_ffi_free_string(error);
    return message;
}

String vortexExpressionToString(const FFI_VortexExpression * expression)
{
    if (!expression)
        return "none";
    char * rendered = vortex_ffi_expr_display(expression);
    if (!rendered)
        return "unprintable";
    String result(rendered);
    vortex_ffi_free_string(rendered);
    return result;
}

std::exception_ptr makeVortexException(const String & message, const std::exception_ptr & callback_exception)
{
    if (callback_exception)
        return callback_exception;
    return std::make_exception_ptr(Exception(ErrorCodes::INCORRECT_DATA, "Error while reading Vortex file: {}", message));
}

[[noreturn]] void throwVortexError(char * error, const std::exception_ptr & callback_exception)
{
    std::rethrow_exception(makeVortexException(takeVortexError(error), callback_exception));
}

/// The I/O options of a reader. A read occupies the thread that runs it until it returns, so
/// allowing more concurrent reads than there are threads to serve them gains nothing; and a buffer
/// that can only seek and read has to be limited to one read at a time. Coalescing reads the gaps
/// between nearby segments in exchange for fewer requests, which pays off on remote storage.
static FFI_VortexReaderOptions makeReaderOptions(const VortexFile & file, size_t io_threads, bool is_remote_fs)
{
    FFI_VortexReaderOptions options{};
    const bool in_memory = file.isInMemory();
    const bool thread_safe = file.isThreadSafe();
    options.io_concurrency = thread_safe ? static_cast<uint32_t>(std::clamp<size_t>(io_threads, 1, 1024)) : 1;
    if (!in_memory)
    {
        options.coalesce_max_gap_bytes = 1 << 20;
        options.coalesce_max_read_bytes = is_remote_fs ? (16 << 20) : (4 << 20);
    }
    return options;
}

FFI_VortexReader * openVortexReader(
    const FFI_VortexRuntime * runtime,
    ReadBuffer & in,
    const FormatSettings & format_settings,
    std::atomic<int> & is_stopped,
    std::unique_ptr<VortexFile> & file,
    VortexReadContext & read_context,
    std::shared_ptr<const ArrowIPC::ArrowSchema> & file_schema,
    size_t io_threads,
    bool is_remote_fs)
{
    file = VortexFile::open(in, format_settings, is_stopped);
    if (is_stopped)
        return nullptr;

    read_context.file = file.get();

    const FFI_VortexReaderOptions options = makeReaderOptions(*file, io_threads, is_remote_fs);
    char * error = nullptr;
    auto * reader
        = vortex_ffi_reader_open(runtime, &read_context, vortexFFIReadCallback, static_cast<uint64_t>(file->size()), &options, &error);
    if (!reader)
        throwVortexError(error, read_context.getException());

    try
    {
        ::ArrowSchema c_schema{};
        if (vortex_ffi_reader_schema(reader, &c_schema, &error) != 0)
            throwVortexError(error, read_context.getException());
        SCOPE_EXIT({
            if (c_schema.release)
                c_schema.release(&c_schema);
        });
        file_schema = std::make_shared<const ArrowIPC::ArrowSchema>(ArrowIPC::importSchemaFromC(c_schema));
    }
    catch (...)
    {
        vortex_ffi_reader_free(reader);
        throw;
    }

    return reader;
}

}

#endif
