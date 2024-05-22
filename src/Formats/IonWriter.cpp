#include "IonWriter.h"

#if USE_ION
#include <decNumber/decContext.h>
#include <Common/formatIPv6.h>

namespace DB
{

namespace ErrorCodes
{
extern const int ION_FORMAT_WRITER_ERROR;
}

#    define ION_CHECK(status, exception_msg) \
        if (status) \
        { \
            throw Exception(ErrorCodes::ION_FORMAT_WRITER_ERROR, "{}, error: {}", exception_msg, ion_error_to_str(status)); \
        }

IonWriter::IonWriter(WriteBuffer & out_, bool output_as_binary_, bool pretty_print_, bool small_containers_in_line_)
    : out(out_), ion_buffer(out.available())
{
    // init output options
    ION_WRITER_OPTIONS ion_options;
    memset(&ion_options, 0, sizeof(ion_options));
    ion_options.output_as_binary = output_as_binary_;
    if (!output_as_binary_)
    {
        ion_options.pretty_print = pretty_print_;
        ion_options.small_containers_in_line = small_containers_in_line_;
    }


    // Callback function called when flushing from an output stream
    // flash is called only if we are at the zero nesting level, that is, not in a list or structure,
    // which means one full ion message will be saved to the internal buffer,
    // and then it will be transferred to WriteBuffer
    ion_writer_callback = [this](_ion_user_stream * stream) -> iERR
    {
        // if limit is init it is call from flush
        if (stream->limit == ion_buffer.data() + ion_buffer.size())
        {
            // Save current total written bytes
            if ((stream->curr >= ion_buffer.data()) && (stream->curr <= ion_buffer.data() + ion_buffer.size()))
                total_written_bytes = stream->curr - ion_buffer.data();

            // Checking whether the buffer has overflowed; if so, expand and update the stream pointers
            if (stream->curr == stream->limit)
            {
                // todo: Should we put a border on top of this?
                ion_buffer.resize(ion_buffer.size() << 1);
                stream->limit = ion_buffer.data() + ion_buffer.size();
                stream->curr = ion_buffer.data() + total_written_bytes;
            }
        }
        else
        {
            // else it is first call
            total_written_bytes = 0;
            stream->limit = ion_buffer.data() + ion_buffer.size();
            stream->curr = ion_buffer.data();
        }
        return IERR_OK;
    };

    // Init stream and writer
    ION_STREAM * stream = nullptr;
    hWRITER writer = nullptr;
    ION_CHECK(ion_stream_open_handler_out(&ionWriteCallback, &ion_writer_callback, &stream), "Cannot create Ion format writer")
    auto err = ion_writer_open(&writer, stream, &ion_options);
    // IERR_OK == 0
    if (err)
    {
        ion_stream_close(stream);
        throw Exception(ErrorCodes::ION_FORMAT_WRITER_ERROR, "Cannot create ION format writer, error: {}", ion_error_to_str(err));
    }

    ion_stream = stream;
    ion_writer = writer;
}

IonWriter::~IonWriter()
{
    if (ion_writer != nullptr)
    {
        ion_writer_close(ion_writer);
        ion_stream_close(ion_stream);
        ion_writer = nullptr;
        ion_stream = nullptr;
    }
    // TODO: Is it necessary to somehow handle errors from ion-c here?
    // ion_writer: if any value is in-progress, closing any writer raises an error, but still frees the writer and any associated memory.
    // (only one possible error)
    // ion_stream: Possible errors from write during the final flush, but if we used callback handler only error from it is possible
    //    if (ion_writer != nullptr)
    //    {
    //        ION_CHECK(ion_writer_close(ion_writer), "Cannot close Ion writer")
    //        ion_writer = nullptr;
    //    }
    //    if (ion_stream != nullptr)
    //    {
    //        ION_CHECK(ion_stream_close(ion_stream), "Cannot close Ion stream")
    //        ion_stream = nullptr;
    //    }
}

size_t IonWriter::bytesWritten() const
{
    return total_written_bytes;
}

size_t IonWriter::flush()
{
    SIZE written_bytes;
    ION_CHECK(ion_writer_flush(ion_writer, &written_bytes), "Cannot flush")
    return written_bytes;
}

size_t IonWriter::finish()
{
    SIZE written_bytes;
    ION_CHECK(ion_writer_finish(ion_writer, &written_bytes), "Cannot finish writing")
    return written_bytes;
}

void IonWriter::final()
{
    size_t written_bytes = finish();
    out.write(reinterpret_cast<char *>(ion_buffer.data()), written_bytes);
}

void IonWriter::writeInt(int64_t value)
{
    ION_CHECK(ion_writer_write_int64(ion_writer, value), "Cannot write int")
}

void IonWriter::writeBool(bool value)
{
    ION_CHECK(ion_writer_write_bool(ion_writer, value), "Cannot write bool")
}

static std::vector<BYTE> getIonEndianBytes(const char * value, size_t n)
{
    std::vector<BYTE> big_int_buffer(n);
    if constexpr (std::endian::native == std::endian::little)
        for (size_t i = 0; i < n; ++i)
            big_int_buffer[n - i - 1] = value[i];
    else
        for (size_t i = 0; i < n; ++i)
            big_int_buffer[i] = value[i];
    return big_int_buffer;
}

void IonWriter::writeBigInt(const char * value, size_t n)
{
    std::vector<BYTE> big_int_buffer = getIonEndianBytes(value, n);
    ION_INT * ion_int_ptr = nullptr;
    ION_CHECK(ion_int_alloc(nullptr, &ion_int_ptr), "Cannot allocate memory for big int")
    // big endian byte order is expected
    ION_CHECK(ion_int_from_bytes(ion_int_ptr, big_int_buffer.data(), static_cast<SIZE>(big_int_buffer.size())), "Cannot create big int")
    ION_CHECK(ion_writer_write_ion_int(ion_writer, ion_int_ptr), "Cannot write big int")
    ion_int_free(ion_int_ptr);
}

void IonWriter::writeBigUInt(const char * value, size_t n)
{
    std::vector<BYTE> big_int_buffer = getIonEndianBytes(value, n);
    ION_INT * ion_int_ptr = nullptr;
    ION_CHECK(ion_int_alloc(nullptr, &ion_int_ptr), "Cannot allocate memory for big int")
    // big endian byte order is expected
    ION_CHECK(
        ion_int_from_abs_bytes(ion_int_ptr, big_int_buffer.data(), static_cast<SIZE>(big_int_buffer.size()), false),
        "Cannot create big int")
    ION_CHECK(ion_writer_write_ion_int(ion_writer, ion_int_ptr), "Cannot write big int")
    ion_int_free(ion_int_ptr);
}

void IonWriter::writeFloat(float value)
{
    ION_CHECK(ion_writer_write_float(ion_writer, value), "Cannot write float")
}

void IonWriter::writeDouble(double value)
{
    ION_CHECK(ion_writer_write_double(ion_writer, value), "Cannot write double")
}

void IonWriter::writeDecimal32(Int32 value)
{
    ION_DECIMAL decimal_value;
    ion_decimal_from_int32(&decimal_value, value); // no errors are possible
    ION_CHECK(ion_writer_write_ion_decimal(ion_writer, &decimal_value), "Cannot write decimal")
    ion_decimal_free(&decimal_value); // no errors are possible
}

void IonWriter::writeDecimal64(Int64 value)
{
    ION_DECIMAL decimal_value;
    decContext set;
    decContextDefault(&set, DEC_INIT_DECIMAL64);
    ION_INT * value_ion_int = nullptr;
    ION_CHECK(ion_int_alloc(nullptr, &value_ion_int), "Cannot allocate memory for big int")
    ION_CHECK(ion_int_from_long(value_ion_int, value), "Cannot create Ion int from int64")
    ION_CHECK(ion_decimal_from_ion_int(&decimal_value, &set, value_ion_int), "Cannot create decimal from Ion int")
    ION_CHECK(ion_writer_write_ion_decimal(ion_writer, &decimal_value), "Cannot write decimal")
    ion_int_free(value_ion_int);
    ion_decimal_free(&decimal_value);
}

void IonWriter::writeDecimal128(const char * value, size_t n)
{
    ION_DECIMAL decimal_value;
    decContext set;
    decContextDefault(&set, DEC_INIT_DECIMAL128);
    std::vector<BYTE> buffer = getIonEndianBytes(value, n);
    SIZE bytes_count = static_cast<SIZE>(n);
    ION_INT * value_ion_int = nullptr;
    ION_CHECK(ion_int_alloc(nullptr, &value_ion_int), "Cannot allocate memory for big int")
    ION_CHECK(ion_int_from_bytes(value_ion_int, buffer.data(), bytes_count), "Cannot create big int")
    ION_CHECK(ion_decimal_from_ion_int(&decimal_value, &set, value_ion_int), "Cannot create decimal from Ion int")
    ION_CHECK(ion_writer_write_ion_decimal(ion_writer, &decimal_value), "Cannot write decimal")
    ion_int_free(value_ion_int);
    ion_decimal_free(&decimal_value);
}

void IonWriter::writeDecimal256(const char * value, size_t n)
{
    ION_DECIMAL decimal_value;
    decContext set;
    decContextDefault(&set, DEC_INIT_DECIMAL128);
    set.digits = IonDecimal256Configs::DigitsCount;
    set.emin = IonDecimal256Configs::MaxNegativeExp;
    set.emax = IonDecimal256Configs::MaxPositiveExp;
    std::vector<BYTE> buffer = getIonEndianBytes(value, n);
    SIZE bytes_count = static_cast<SIZE>(n);
    ION_INT * value_ion_int = nullptr;
    ION_CHECK(ion_int_alloc(nullptr, &value_ion_int), "Cannot allocate memory for big int")
    ION_CHECK(ion_int_from_bytes(value_ion_int, buffer.data(), bytes_count), "Cannot create big int")
    ION_CHECK(ion_decimal_from_ion_int(&decimal_value, &set, value_ion_int), "Cannot create decimal from Ion int")
    ION_CHECK(ion_writer_write_ion_decimal(ion_writer, &decimal_value), "Cannot write decimal")
    ion_int_free(value_ion_int);
    ion_decimal_free(&decimal_value);
}

void IonWriter::writeString(const std::string_view str_view)
{
    ION_STRING ion_str;
    ion_string_assign_cstr(&ion_str, const_cast<char *>(str_view.data()), static_cast<SIZE>(str_view.size()));
    ION_CHECK(ion_writer_write_string(ion_writer, &ion_str), "Cannot write string")
}

void IonWriter::writeDate(int year, int month, int day)
{
    ION_TIMESTAMP date_obj;
    ION_CHECK(ion_timestamp_for_day(&date_obj, year, month, day), "Cannot create timestamp object with day precision")
    ION_CHECK(ion_writer_write_timestamp(ion_writer, &date_obj), "Cannot write timestamp")
}

void IonWriter::writeDateTime(int year, int month, int day, int hours, int minutes, int seconds)
{
    ION_TIMESTAMP datetime_obj;
    ION_CHECK(
        ion_timestamp_for_second(&datetime_obj, year, month, day, hours, minutes, seconds),
        "Cannot create timestamp object with seconds precision")
    ION_CHECK(ion_writer_write_timestamp(ion_writer, &datetime_obj), "Cannot write timestamp")
}

void IonWriter::writeIPv4(const IPv4 & ipv4)
{
    std::array<char, IPV4_MAX_TEXT_LENGTH> addr;
    char * paddr = addr.data();
    formatIPv4(reinterpret_cast<const unsigned char *>(&ipv4), paddr);
    SIZE addr_size = static_cast<SIZE>(paddr - addr.data() - 1);
    ION_STRING ion_str;
    ion_string_assign_cstr(&ion_str, addr.data(), addr_size);
    ION_CHECK(ion_writer_write_string(ion_writer, &ion_str), "Cannot write IPv4 string")
}

void IonWriter::writeIPv6(const IPv6 & ipv6)
{
    std::array<char, IPV4_MAX_TEXT_LENGTH> addr;
    char * paddr = addr.data();
    formatIPv6(reinterpret_cast<const unsigned char *>(&ipv6), paddr);
    SIZE addr_size = static_cast<SIZE>(paddr - addr.data() - 1);
    ION_STRING ion_str;
    ion_string_assign_cstr(&ion_str, addr.data(), addr_size);
    ION_CHECK(ion_writer_write_string(ion_writer, &ion_str), "Cannot write IPv6 string")
}

void IonWriter::writeStartList()
{
    ION_CHECK(ion_writer_start_container(ion_writer, tid_LIST), "Cannot start writing of list")
}

void IonWriter::writeFinishList()
{
    ION_CHECK(ion_writer_finish_container(ion_writer), "Cannot finish writing of list")
}

void IonWriter::writeTypedNull(NullableIonDataType type)
{
    switch (type)
    {
        case NullableIonDataType::Integer: {
            ION_CHECK(ion_writer_write_typed_null(ion_writer, tid_INT), "Cannot write null.int")
            return;
        }
        case NullableIonDataType::Bool: {
            ION_CHECK(ion_writer_write_typed_null(ion_writer, tid_BOOL), "Cannot write null.bool")
            return;
        }
        case NullableIonDataType::Float: {
            ION_CHECK(ion_writer_write_typed_null(ion_writer, tid_FLOAT), "Cannot write null.float")
            return;
        }
        case NullableIonDataType::Decimal: {
            ION_CHECK(ion_writer_write_typed_null(ion_writer, tid_DECIMAL), "cannot write null.decimal")
            return;
        }
        case NullableIonDataType::DateTime: {
            ION_CHECK(ion_writer_write_typed_null(ion_writer, tid_TIMESTAMP), "Cannot write null.timestamp")
            return;
        }
        case NullableIonDataType::String: {
            ION_CHECK(ion_writer_write_typed_null(ion_writer, tid_STRING), "Cannot write null.string")
            return;
        }
    }
}

void IonWriter::writeNull()
{
    ION_CHECK(ion_writer_write_null(ion_writer), "Cannot write null")
}

void IonWriter::writeStartStruct()
{
    ION_CHECK(ion_writer_start_container(ion_writer, tid_STRUCT), "Cannot start writing of struct")
}

void IonWriter::writeStructFieldName(const std::string_view str_view)
{
    ION_STRING field_name;
    ion_string_assign_cstr(&field_name, const_cast<char *>(str_view.data()), static_cast<SIZE>(str_view.size()));
    ION_CHECK(ion_writer_write_field_name(ion_writer, &field_name), "Cannot write field name")
}

void IonWriter::writeFinishStruct()
{
    ION_CHECK(ion_writer_finish_container(ion_writer), "Cannot finish writing of struct")
}
}

#endif
