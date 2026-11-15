#include <Processors/Formats/Impl/Vortex/VortexBlockOutputFormat.h>

#if USE_VORTEX

#include <Formats/FormatFactory.h>
#include <IO/WriteBuffer.h>
#include <Processors/Formats/Impl/ArrowBatchRowLimit.h>
#include <Processors/Formats/Impl/ArrowIPC/ArrowCData.h>
#include <base/scope_guard.h>
#include <Processors/Port.h>
#include <Common/Exception.h>
#include <Common/isValidUTF8.h>


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

/// Vortex takes the values of a `Utf8` array on trust.
static void checkStringsAreValidUTF8(const ::ArrowArray & array, const ::ArrowSchema & schema, const String & column_name)
{
    if (std::string_view(schema.format) == "u" && array.length > 0)
    {
        const auto * offsets = static_cast<const int32_t *>(array.buffers[1]) + array.offset;
        const auto * bytes = static_cast<const UInt8 *>(array.buffers[2]);
        const int64_t rows = array.length;
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

    for (int64_t i = 0; i < array.n_children; ++i)
        checkStringsAreValidUTF8(*array.children[i], *schema.children[i], column_name);
}

static FormatSettings getArrowFormatSettings(const FormatSettings & format_settings)
{
    FormatSettings result = format_settings;
    result.arrow.output_string_as_string = format_settings.vortex.output_string_as_string;
    /// There is no fixed-width binary type on the Vortex side, so `FixedString` is written as `Binary`.
    result.arrow.output_fixed_string_as_fixed_byte_array = false;
    result.arrow.output_fixed_string_as_string = false;
    /// As a plain `U32` a `DateTime` would come back as a number; as a timestamp with second
    /// precision it survives the round trip, arriving as `DateTime64(0)`.
    result.arrow.output_datetime_as_timestamp = true;
    /// The `Nothing` type (`SELECT NULL`) has a counterpart of its own: the Vortex `Null` type.
    result.arrow.output_nothing_as_null = true;
    result.arrow.low_cardinality_as_dictionary = false;
    return result;
}

VortexBlockOutputFormat::VortexBlockOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_)
    : IOutputFormat(header_, out_)
    , format_settings(format_settings_)
    , arrow_format_settings(getArrowFormatSettings(format_settings_))
{
}

VortexBlockOutputFormat::~VortexBlockOutputFormat()
{
    if (writer)
        vortex_ffi_writer_free(writer);
}

void VortexBlockOutputFormat::initWriter()
{
    const auto & header = getPort(PortKind::Main).getHeader();
    ::ArrowSchema c_schema{};
    ArrowIPC::exportSchemaToC(header.getNames(), header.getDataTypes(), arrow_format_settings, c_schema);
    /// The library takes the schema over (it moves the struct out without marking ours released), so it is
    /// released here only when an exception comes before it is handed over.
    bool handed_over = false;
    SCOPE_EXIT({
        if (!handed_over)
            c_schema.release(&c_schema);
    });

    write_context = std::make_unique<VortexWriteContext>();
    write_context->out = &out;

    char * error = nullptr;
    handed_over = true;
    writer = vortex_ffi_writer_create(write_context.get(), vortexFFIWriteCallback, &c_schema, &error);
    if (!writer)
        throwVortexError(error, write_context->exception);
}

void VortexBlockOutputFormat::consume(Chunk chunk)
{
    const size_t num_rows = chunk.getNumRows();
    if (!num_rows)
        return;

    if (!writer)
        initWriter();

    const auto & header = getPort(PortKind::Main).getHeader();
    const Names names = header.getNames();
    const DataTypes types = header.getDataTypes();
    Columns columns = chunk.detachColumns();
    for (auto & column : columns)
        column = column->convertToFullColumnIfConst()->convertToFullColumnIfSparse()->convertToFullColumnIfReplicated();

    /// Arrow record batches have a 32-bit offset limit, so a chunk too large for one batch is written as several.
    size_t begin = 0;
    do
    {
        size_t rows = maxRowsFittingOneArrowBatch(
            columns,
            types,
            begin,
            num_rows,
            arrow_format_settings.arrow.output_fixed_string_as_fixed_byte_array,
            arrow_format_settings.arrow.output_max_record_batch_bytes);
        /// A row that does not fit on its own cannot be represented at all; let the export reject it.
        rows = std::min(std::max<size_t>(rows, 1), num_rows - begin);

        ::ArrowArray c_array{};
        ::ArrowSchema c_schema{};
        /// The library takes both over (it moves the structs out without marking ours released), so they are
        /// released here only when an exception comes before they are handed over.
        bool handed_over = false;
        SCOPE_EXIT({
            if (!handed_over && c_array.release)
                c_array.release(&c_array);
            if (!handed_over && c_schema.release)
                c_schema.release(&c_schema);
        });
        ArrowIPC::exportSchemaToC(names, types, arrow_format_settings, c_schema);
        ArrowIPC::exportRecordBatchToC(columns, types, arrow_format_settings, begin, begin + rows, c_array);
        begin += rows;

        if (format_settings.vortex.output_string_as_string)
        {
            for (int64_t i = 0; i < c_array.n_children; ++i)
                checkStringsAreValidUTF8(*c_array.children[i], *c_schema.children[i], names[i]);
        }

        char * error = nullptr;
        handed_over = true;
        if (vortex_ffi_writer_write(writer, &c_array, &c_schema, &error) != 0)
            throwVortexError(error, write_context->exception);
    } while (begin < num_rows);
}

void VortexBlockOutputFormat::finalizeImpl()
{
    /// Not a single row came through, but the file still has to be a valid one with the header's
    /// schema in it.
    if (!writer)
        initWriter();

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
