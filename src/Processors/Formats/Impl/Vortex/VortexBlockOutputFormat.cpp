#include <Processors/Formats/Impl/Vortex/VortexBlockOutputFormat.h>

#if USE_VORTEX

#include <Formats/FormatFactory.h>
#include <IO/WriteBuffer.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Processors/Formats/Impl/CHColumnToArrowColumn.h>
#include <Processors/Port.h>
#include <Common/Exception.h>
#include <Common/isValidUTF8.h>

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

/// Vortex takes the values of a `Utf8` array on trust.
static void checkStringsAreValidUTF8(const std::shared_ptr<arrow::ArrayData> & data, const String & column_name)
{
    if (data->type->id() == arrow::Type::STRING && data->length > 0)
    {
        arrow::StringArray strings(data);
        const int64_t rows = strings.length();
        const int32_t * offsets = strings.raw_value_offsets();
        const auto * bytes = reinterpret_cast<const UInt8 *>(strings.raw_data());
        const int32_t begin = offsets[0];
        const int32_t end = offsets[rows];
        if (begin != end)
        {
            /// The values are valid one by one iff their concatenation is valid and none starts inside a code point.
            bool valid = UTF8::isValidUTF8(bytes + begin, end - begin);
            for (int64_t row = 1; valid && row < rows; ++row)
                valid = offsets[row] == end || (bytes[offsets[row]] & 0xC0) != 0x80;
            if (!valid)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Cannot write a value of column `{}` that is not valid UTF-8 to the Vortex `Utf8` type. "
                    "Set `output_format_vortex_string_as_string = 0` to write `String` columns as `Binary`",
                    column_name);
        }
    }

    for (const auto & child : data->child_data)
        checkStringsAreValidUTF8(child, column_name);
    if (data->dictionary)
        checkStringsAreValidUTF8(data->dictionary, column_name);
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
    arrow_settings.output_string_as_string = format_settings.vortex.output_string_as_string;
    /// There is no fixed-width binary type on the Vortex side, so `FixedString` is written as `Binary`.
    arrow_settings.output_fixed_string_as_fixed_byte_array = false;
    arrow_settings.output_fixed_string_as_string = false;
    /// As a plain `U32` a `DateTime` would come back as a number; as a timestamp with second
    /// precision it survives the round trip, arriving as `DateTime64(0)`.
    arrow_settings.output_datetime_as_timestamp = true;
    /// The `Nothing` type (`SELECT NULL`) has a counterpart of its own: the Vortex `Null` type.
    arrow_settings.output_nothing_as_null = true;
    /// A chunk is handed to Vortex as Arrow record batches, which have the same 32-bit offset limit.
    arrow_settings.output_max_record_batch_bytes = format_settings.arrow.output_max_record_batch_bytes;

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

    Chunks chunks;
    chunks.push_back(std::move(chunk));
    ch_column_to_arrow_column->chChunkToArrowTable(arrow_table, chunks, columns_num);

    if (format_settings.vortex.output_string_as_string)
        for (int i = 0; i < arrow_table->num_columns(); ++i)
            for (const auto & array : arrow_table->column(i)->chunks())
                checkStringsAreValidUTF8(array->data(), arrow_table->field(i)->name());

    /// A chunk too large for one Arrow record batch comes back as several table chunks. Recombining them
    /// would hit the same 32-bit offset limit, so write each record batch separately.
    arrow::TableBatchReader batch_reader(*arrow_table);
    while (true)
    {
        std::shared_ptr<arrow::RecordBatch> batch;
        throwFromArrowStatusIfFailed(batch_reader.ReadNext(&batch));
        if (!batch)
            break;

        ArrowArray c_array{};
        ArrowSchema c_schema{};
        throwFromArrowStatusIfFailed(arrow::ExportRecordBatch(*batch, &c_array, &c_schema));

        char * error = nullptr;
        if (vortex_ffi_writer_write(writer, &c_array, &c_schema, &error) != 0)
            throwVortexError(error, write_context->exception);
    }
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
