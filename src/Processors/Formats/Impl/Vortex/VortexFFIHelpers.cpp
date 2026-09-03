#include <Processors/Formats/Impl/Vortex/VortexFFIHelpers.h>

#if USE_VORTEX

#include <IO/ReadBuffer.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Common/Exception.h>

#include <arrow/c/bridge.h>
#include <arrow/io/interfaces.h>
#include <arrow/io/memory.h>
#include <arrow/result.h>

#include <algorithm>

#include <vortex_ffi.h>

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
        auto result = ctx->file->ReadAt(static_cast<int64_t>(offset), static_cast<int64_t>(length), out);
        if (!result.ok())
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Error while reading Vortex file: {}", result.status().ToString());
        if (*result != static_cast<int64_t>(length))
            throw Exception(
                ErrorCodes::CANNOT_READ_ALL_DATA,
                "Unexpected end of Vortex file: read {} bytes instead of {} at offset {}",
                *result,
                length,
                offset);
        ctx->bytes_read.fetch_add(length, std::memory_order_relaxed);
        return 0;
    }
    catch (...)
    {
        ctx->setException(std::current_exception());
        return 1;
    }
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

void throwFromArrowStatusIfFailed(const arrow::Status & status)
{
    if (!status.ok())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Error while reading Vortex file: {}", status.ToString());
}

/// The I/O options of a reader. A read occupies the thread that runs it until it returns, so
/// allowing more concurrent reads than there are threads to serve them gains nothing; and a buffer
/// that can only seek and read has to be limited to one read at a time. Coalescing reads the gaps
/// between nearby segments in exchange for fewer requests, which pays off on remote storage.
static FFI_VortexReaderOptions makeReaderOptions(const arrow::io::RandomAccessFile & file, size_t io_threads, bool is_remote_fs)
{
    FFI_VortexReaderOptions options{};
    const bool in_memory = dynamic_cast<const arrow::io::BufferReader *>(&file) != nullptr;
    const bool thread_safe = in_memory || dynamic_cast<const RandomAccessFileFromRandomAccessReadBuffer *>(&file) != nullptr;
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
    std::shared_ptr<arrow::io::RandomAccessFile> & arrow_file,
    VortexReadContext & read_context,
    std::shared_ptr<arrow::Schema> & file_schema,
    size_t io_threads,
    bool is_remote_fs)
{
    /// Read-ahead here would only fetch bytes the library never asked for: it chooses the byte
    /// ranges itself and merges the nearby ones.
    arrow_file = asArrowFile(in, format_settings, is_stopped, "Vortex", VORTEX_MAGIC_BYTES, /* avoid_buffering */ true);
    if (is_stopped)
        return nullptr;

    auto file_size = arrow_file->GetSize();
    throwFromArrowStatusIfFailed(file_size.status());

    read_context.file = arrow_file.get();

    const FFI_VortexReaderOptions options = makeReaderOptions(*arrow_file, io_threads, is_remote_fs);
    char * error = nullptr;
    auto * reader
        = vortex_ffi_reader_open(runtime, &read_context, vortexFFIReadCallback, static_cast<uint64_t>(*file_size), &options, &error);
    if (!reader)
        throwVortexError(error, read_context.getException());

    ArrowSchema c_schema{};
    if (vortex_ffi_reader_schema(reader, &c_schema, &error) != 0)
    {
        vortex_ffi_reader_free(reader);
        throwVortexError(error, read_context.getException());
    }

    auto schema = arrow::ImportSchema(&c_schema);
    if (!schema.ok())
    {
        vortex_ffi_reader_free(reader);
        throwFromArrowStatusIfFailed(schema.status());
    }
    file_schema = *schema;

    return reader;
}

}

#endif
