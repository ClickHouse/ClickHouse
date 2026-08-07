#include <Processors/Formats/Impl/VortexBlockOutputFormat.h>

#if USE_VORTEX

#include <Formats/FormatFactory.h>
#include <IO/WriteBuffer.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Processors/Formats/Impl/CHColumnToArrowColumn.h>
#include <Processors/Port.h>
#include <Common/Exception.h>

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/result.h>

#include <vortex_ffi.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_WRITE_TO_OSTREAM;
    extern const int INCORRECT_DATA;
}

/// The context of the write callback passed to the Rust vortex library. The library calls the
/// callback only from inside FFI calls, on the calling thread, so no synchronization is needed.
struct VortexWriteContext
{
    WriteBuffer * out = nullptr;
    std::exception_ptr exception;
};

extern "C" int32_t vortexFFIWriteCallback(void * context, const uint8_t * data, uint64_t length);
extern "C" int32_t vortexFFIWriteCallback(void * context, const uint8_t * data, uint64_t length)
{
    auto * ctx = static_cast<VortexWriteContext *>(context);
    try
    {
        ctx->out->write(reinterpret_cast<const char *>(data), length);
        return 0;
    }
    catch (...)
    {
        ctx->exception = std::current_exception();
        return 1;
    }
}

[[noreturn]] static void throwVortexError(char * error, const std::exception_ptr & callback_exception)
{
    String message = error ? String(error) : "unknown error";
    if (error)
        vortex_ffi_free_string(error);
    if (callback_exception)
        std::rethrow_exception(callback_exception);
    throw Exception(ErrorCodes::CANNOT_WRITE_TO_OSTREAM, "Error while writing Vortex file: {}", message);
}

static void throwFromArrowStatusIfFailed(const arrow::Status & status)
{
    if (!status.ok())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Error while writing Vortex file: {}", status.ToString());
}

VortexBlockOutputFormat::VortexBlockOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_)
    : IOutputFormat(header_, out_)
    , format_settings(format_settings_)
{
}

VortexBlockOutputFormat::~VortexBlockOutputFormat()
{
    if (writer)
        vortex_ffi_writer_free(writer);
}

void VortexBlockOutputFormat::initWriter(const Chunk * chunk)
{
    CHColumnToArrowColumn::Settings arrow_settings;
    /// ClickHouse strings are arbitrary byte sequences, while Vortex validates that `Utf8` values
    /// are valid UTF-8 (e.g. when computing statistics), so strings must be written as `Binary`.
    arrow_settings.output_string_as_string = false;
    /// Vortex has no fixed-size binary type.
    arrow_settings.output_fixed_string_as_fixed_byte_array = false;
    /// Write `DateTime` as `vortex.timestamp` with second precision instead of the generic `U32`,
    /// so the temporal type is preserved on round-trip (it is read back as `DateTime64(0)`).
    arrow_settings.output_datetime_as_timestamp = true;
    /// Write the `Nothing` type (e.g. `SELECT NULL`) as the Vortex `Null` type.
    arrow_settings.output_nothing_as_null = true;

    ch_column_to_arrow_column
        = std::make_unique<CHColumnToArrowColumn>(getPort(PortKind::Main).getHeader(), "Vortex", arrow_settings);
    ch_column_to_arrow_column->initializeArrowSchema(chunk);
    auto arrow_schema = ch_column_to_arrow_column->getArrowSchema();

    ArrowSchema c_schema{};
    throwFromArrowStatusIfFailed(arrow::ExportSchema(*arrow_schema, &c_schema));

    write_context = std::make_unique<VortexWriteContext>();
    write_context->out = &out;

    char * error = nullptr;
    writer = vortex_ffi_writer_create(write_context.get(), vortexFFIWriteCallback, &c_schema, &error);
    if (!writer)
        throwVortexError(error, write_context->exception);
}

void VortexBlockOutputFormat::consume(Chunk chunk)
{
    if (!chunk.getNumRows())
        return;

    if (!writer)
        initWriter(&chunk);

    size_t columns_num = chunk.getNumColumns();
    std::shared_ptr<arrow::Table> arrow_table;

    std::vector<Chunk> chunks;
    chunks.push_back(std::move(chunk));
    ch_column_to_arrow_column->chChunkToArrowTable(arrow_table, chunks, columns_num);

    auto batch = arrow_table->CombineChunksToBatch(ArrowMemoryPool::instance());
    throwFromArrowStatusIfFailed(batch.status());

    ArrowArray c_array{};
    ArrowSchema c_schema{};
    throwFromArrowStatusIfFailed(arrow::ExportRecordBatch(**batch, &c_array, &c_schema));

    char * error = nullptr;
    if (vortex_ffi_writer_write(writer, &c_array, &c_schema, &error) != 0)
        throwVortexError(error, write_context->exception);
}

void VortexBlockOutputFormat::finalizeImpl()
{
    /// If no rows were written, produce a valid empty file with the schema from the header.
    if (!writer)
        initWriter(nullptr);

    char * error = nullptr;
    if (vortex_ffi_writer_finish(writer, &error) != 0)
        throwVortexError(error, write_context->exception);
}

void VortexBlockOutputFormat::resetFormatterImpl()
{
    /// The formatter can be reused to write another file into the same output buffer (this is how
    /// `MessageQueueSink` formats every message). The Rust writer is consumed by
    /// `vortex_ffi_writer_finish`, so drop it and start the next file from scratch.
    if (writer)
    {
        vortex_ffi_writer_free(writer);
        writer = nullptr;
    }
    ch_column_to_arrow_column.reset();
    write_context.reset();
}

void registerOutputFormatVortex(FormatFactory & factory);
void registerOutputFormatVortex(FormatFactory & factory)
{
    factory.registerOutputFormat(
        "Vortex",
        [](WriteBuffer & buf,
           const Block & sample,
           const FormatSettings & format_settings,
           FormatFilterInfoPtr /*format_filter_info*/) -> OutputFormatPtr
        {
            return std::make_shared<VortexBlockOutputFormat>(buf, std::make_shared<const Block>(sample), format_settings);
        });
    factory.markFormatHasNoAppendSupport("Vortex");
    factory.markOutputFormatNotTTYFriendly("Vortex");
    factory.setContentType("Vortex", "application/octet-stream");
}

}

#else

namespace DB
{
class FormatFactory;
void registerOutputFormatVortex(FormatFactory &);
void registerOutputFormatVortex(FormatFactory &) {}
}

#endif
