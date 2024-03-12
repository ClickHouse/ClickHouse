#pragma once

#include "config.h"

#if USE_ION
#    include <Core/Types.h>
#    include <IO/WriteBuffer.h>
#    include <Common/PODArray.h>

#    include <ionc/ion.h>

namespace DB
{

enum class NullableIonDataType : std::uint8_t
{
    Integer,
    Float,
    Decimal,
    DateTime,
    String
};

/// Utility class for writing in the Ion format.
class IonWriter
{
private:
    WriteBuffer & out;

    std::vector<BYTE> ion_buffer;
    hWRITER ion_writer = nullptr;
    ION_STREAM * ion_stream = nullptr;

    using IonWriteCallback = std::function<iERR(_ion_user_stream * stream)>;
    IonWriteCallback ion_writer_callback{};
    size_t total_written_bytes = 0;

    static iERR ionWriteCallback(_ion_user_stream * stream)
    {
        // forwards call to ion_writer_callback
        return (*reinterpret_cast<IonWriteCallback *>(stream->handler_state))(stream);
    }

    enum IonDecimal256Configs : std::int32_t
    {
        DigitsCount = 76,
        MaxPositiveExp = 1572864,
        MaxNegativeExp = -1572863
    };

public:
    explicit IonWriter(WriteBuffer & out_, bool output_as_binary_);
    ~IonWriter();

    // Writes methods
    void writeInt(Int64 value);
    void writeBigInt(const char * value, size_t n);
    void writeBigUInt(const char * value, size_t n);
    void writeFloat(float value);
    void writeDouble(double value);
    void writeDecimal32(Int32 value);
    void writeDecimal64(Int64 value);
    void writeDecimal128(const char * value, size_t n);
    void writeDecimal256(const char * value, size_t n);
    void writeString(std::string_view str_view);
    void writeDate(int year, int month, int day);
    void writeDateTime(int year, int month, int day, int hours, int minutes, int seconds);
    void writeIPv4(const IPv4 & ipv4);
    void writeIPv6(const IPv6 & ipv6);
    void writeStartList();
    void writeFinishList();
    void writeTypedNull(NullableIonDataType type);
    void writeNull();
    void writeStartStruct();
    void writeStructFieldName(std::string_view str_view);
    void writeFinishStruct();

    void final();
    size_t bytesWritten() const;

private:
    size_t flush();
    size_t finish();
};

}
#endif
