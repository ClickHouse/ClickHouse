#include "IonWriter.h"

#if USE_ION
#    include <iostream>
#    include <decNumber/decContext.h>
#    include <Common/formatIPv6.h>

namespace DB
{

IonWriter::IonWriter(WriteBuffer & out_, bool output_as_binary_) : out(out_), ion_buffer(out.available())
{
    // init output options
    ION_WRITER_OPTIONS ion_options;
    memset(&ion_options, 0, sizeof(ion_options));
    ion_options.output_as_binary = FALSE;
    if (!output_as_binary_)
        ion_options.pretty_print = TRUE;

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
    auto err = ion_stream_open_handler_out(&ionWriteCallback, &ion_writer_callback, &stream);
    if (err == IERR_OK)
    {
        err = ion_writer_open(&writer, stream, &ion_options);
        if (err == IERR_OK)
        {
            ion_stream = stream;
            ion_writer = writer;
        }
        else
        {
            ion_stream_close(stream);
            // todo: Should we be doing anything else here?
        }
    }
}

IonWriter::~IonWriter()
{
    if (ion_writer != nullptr)
    {
        ion_writer_close(ion_writer);
        ion_stream_close(ion_stream);
    }
}

size_t IonWriter::bytesWritten() const
{
    return total_written_bytes;
}

size_t IonWriter::flush()
{
    SIZE written_bytes;
    ion_writer_flush(ion_writer, &written_bytes);
    return written_bytes;
}

size_t IonWriter::finish()
{
    SIZE written_bytes;
    ion_writer_finish(ion_writer, &written_bytes);
    return written_bytes;
}

void IonWriter::final()
{
    size_t written_bytes = finish();
    out.write(reinterpret_cast<char *>(ion_buffer.data()), written_bytes);
}

void IonWriter::writeInt(int64_t value)
{
    ion_writer_write_int64(ion_writer, value);
}

void IonWriter::writeBigInt(const char * value, size_t n)
{
    std::vector<BYTE> big_int_buffer(n);
    for (size_t i = 0; i < n; ++i)
        big_int_buffer[n - i - 1] = value[i];
    ION_INT * ion_int_ptr = nullptr;
    ion_int_alloc(nullptr, &ion_int_ptr);
    ion_int_from_bytes(ion_int_ptr, big_int_buffer.data(), static_cast<SIZE>(big_int_buffer.size()));
    ion_writer_write_ion_int(ion_writer, ion_int_ptr);
}

void IonWriter::writeBigUInt(const char * value, size_t n)
{
    std::vector<BYTE> big_int_buffer(n);
    for (size_t i = 0; i < n; ++i)
        big_int_buffer[n - i - 1] = value[i];
    ION_INT * ion_int_ptr = nullptr;
    ion_int_alloc(nullptr, &ion_int_ptr);
    ion_int_from_abs_bytes(ion_int_ptr, big_int_buffer.data(), static_cast<SIZE>(big_int_buffer.size()), false);
    ion_writer_write_ion_int(ion_writer, ion_int_ptr);
}

void IonWriter::writeFloat(float value)
{
    ion_writer_write_float(ion_writer, value);
}

void IonWriter::writeDouble(double value)
{
    ion_writer_write_double(ion_writer, value);
}

void IonWriter::writeDecimal32(Int32 value)
{
    ION_DECIMAL decimal_value;
    ion_decimal_from_int32(&decimal_value, value);
    ion_writer_write_ion_decimal(ion_writer, &decimal_value);
    ion_decimal_free(&decimal_value);
}

void IonWriter::writeDecimal64(Int64 value)
{
    ION_DECIMAL decimal_value;
    decContext set;
    decContextDefault(&set, DEC_INIT_DECIMAL64);
    ION_INT * value_ion_int = nullptr;
    ion_int_alloc(nullptr, &value_ion_int);
    ion_int_from_long(value_ion_int, value);
    ion_decimal_from_ion_int(&decimal_value, &set, value_ion_int);
    ion_writer_write_ion_decimal(ion_writer, &decimal_value);
    ion_int_free(value_ion_int);
    ion_decimal_free(&decimal_value);
}

void IonWriter::writeDecimal128(const char * value, size_t n)
{
    ION_DECIMAL decimal_value;
    decContext set;
    decContextDefault(&set, DEC_INIT_DECIMAL128);
    std::vector<BYTE> buffer(n);
    for (size_t i = 0; i < n; ++i)
        buffer[n - i - 1] = value[i];
    SIZE bytes_count = static_cast<SIZE>(n);
    ION_INT * value_ion_int = nullptr;
    ion_int_alloc(nullptr, &value_ion_int);
    ion_int_from_bytes(value_ion_int, buffer.data(), bytes_count);
    ion_decimal_from_ion_int(&decimal_value, &set, value_ion_int);
    ion_writer_write_ion_decimal(ion_writer, &decimal_value);
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
    std::vector<BYTE> buffer(n);
    for (size_t i = 0; i < n; ++i)
        buffer[n - i - 1] = value[i];
    SIZE bytes_count = static_cast<SIZE>(n);
    ION_INT * value_ion_int = nullptr;
    ion_int_alloc(nullptr, &value_ion_int);
    ion_int_from_bytes(value_ion_int, buffer.data(), bytes_count);
    ion_decimal_from_ion_int(&decimal_value, &set, value_ion_int);
    ion_writer_write_ion_decimal(ion_writer, &decimal_value);
    ion_int_free(value_ion_int);
    ion_decimal_free(&decimal_value);
}

void IonWriter::writeString(const std::string_view str_view)
{
    ION_STRING ion_str;
    ion_string_assign_cstr(&ion_str, const_cast<char *>(str_view.data()), static_cast<SIZE>(str_view.size()));
    ion_writer_write_string(ion_writer, &ion_str);
}

void IonWriter::writeDate(int year, int month, int day)
{
    ION_TIMESTAMP date_obj;
    ion_timestamp_for_day(&date_obj, year, month, day);
    ion_writer_write_timestamp(ion_writer, &date_obj);
}

void IonWriter::writeDateTime(int year, int month, int day, int hours, int minutes, int seconds)
{
    ION_TIMESTAMP date_obj;
    ion_timestamp_for_second(&date_obj, year, month, day, hours, minutes, seconds);
    ion_writer_write_timestamp(ion_writer, &date_obj);
}

void IonWriter::writeIPv4(const IPv4 & ipv4)
{
    std::array<char, IPV4_MAX_TEXT_LENGTH> addr;
    char * paddr = addr.data();
    formatIPv4(reinterpret_cast<const unsigned char *>(&ipv4), paddr);
    SIZE addr_size = static_cast<SIZE>(paddr - addr.data() - 1);
    ION_STRING ion_str;
    ion_string_assign_cstr(&ion_str, addr.data(), addr_size);
    ion_writer_write_string(ion_writer, &ion_str);
}

void IonWriter::writeIPv6(const IPv6 & ipv6)
{
    std::array<char, IPV4_MAX_TEXT_LENGTH> addr;
    char * paddr = addr.data();
    formatIPv6(reinterpret_cast<const unsigned char *>(&ipv6), paddr);
    SIZE addr_size = static_cast<SIZE>(paddr - addr.data() - 1);
    ION_STRING ion_str;
    ion_string_assign_cstr(&ion_str, addr.data(), addr_size);
    ion_writer_write_string(ion_writer, &ion_str);
}

void IonWriter::writeStartList()
{
    ion_writer_start_container(ion_writer, tid_LIST);
}

void IonWriter::writeFinishList()
{
    ion_writer_finish_container(ion_writer);
}

void IonWriter::writeTypedNull(NullableIonDataType type)
{
    switch (type)
    {
        case NullableIonDataType::Integer: {
            ion_writer_write_typed_null(ion_writer, tid_INT);
            return;
        }
        case NullableIonDataType::Float: {
            ion_writer_write_typed_null(ion_writer, tid_FLOAT);
            return;
        }
        case NullableIonDataType::Decimal: {
            ion_writer_write_typed_null(ion_writer, tid_DECIMAL);
            return;
        }
        case NullableIonDataType::DateTime: {
            ion_writer_write_typed_null(ion_writer, tid_TIMESTAMP);
            return;
        }
        case NullableIonDataType::String: {
            ion_writer_write_typed_null(ion_writer, tid_STRING);
            return;
        }
    }
}

void IonWriter::writeNull()
{
    ion_writer_write_null(ion_writer);
}

void IonWriter::writeStartStruct()
{
    ion_writer_start_container(ion_writer, tid_STRUCT);
}

void IonWriter::writeStructFieldName(const std::string_view str_view)
{
    ION_STRING field_name;
    ion_string_assign_cstr(&field_name, const_cast<char *>(str_view.data()), static_cast<SIZE>(str_view.size()));
    ion_writer_write_field_name(ion_writer, &field_name);
}

void IonWriter::writeFinishStruct()
{
    ion_writer_finish_container(ion_writer);
}
}

#endif
