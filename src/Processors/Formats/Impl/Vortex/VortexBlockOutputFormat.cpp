#include <Processors/Formats/Impl/Vortex/VortexBlockOutputFormat.h>

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

/// What the write callback needs. Writing runs entirely on the thread that called us, so there is
/// nothing here to synchronize.
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
    /// A ClickHouse string is any sequence of bytes, while Vortex insists that `Utf8` really is
    /// UTF-8, so strings leave as `Binary`.
    arrow_settings.output_string_as_string = false;
    /// There is no fixed-width binary type on the Vortex side either.
    arrow_settings.output_fixed_string_as_fixed_byte_array = false;
    /// As a plain `U32` a `DateTime` would come back as a number; as a timestamp with second
    /// precision it survives the round trip, arriving as `DateTime64(0)`.
    arrow_settings.output_datetime_as_timestamp = true;
    /// The `Nothing` type (`SELECT NULL`) has a counterpart of its own: the Vortex `Null` type.
    arrow_settings.output_nothing_as_null = true;

    ch_column_to_arrow_column = std::make_unique<CHColumnToArrowColumn>(getPort(PortKind::Main).getHeader(), "Vortex", arrow_settings);
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
    /// Not a single row came through, but the file still has to be a valid one with the header's
    /// schema in it.
    if (!writer)
        initWriter(nullptr);

    char * error = nullptr;
    if (vortex_ffi_writer_finish(writer, &error) != 0)
        throwVortexError(error, write_context->exception);
}

void VortexBlockOutputFormat::resetFormatterImpl()
{
    /// The same formatter may be asked for another file into the same buffer - this is how
    /// `MessageQueueSink` formats every message - and finishing the previous one has already
    /// consumed the Rust writer.
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
        [](WriteBuffer & buf, const Block & sample, const FormatSettings & format_settings, FormatFilterInfoPtr /* format_filter_info */)
            -> OutputFormatPtr
        { return std::make_shared<VortexBlockOutputFormat>(buf, std::make_shared<const Block>(sample), format_settings); });
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
void registerOutputFormatVortex(FormatFactory &)
{
}
}

#endif
