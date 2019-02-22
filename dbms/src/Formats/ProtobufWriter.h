#pragma once

#include <common/DayNum.h>
#include <Common/UInt128.h>
#include <Core/UUID.h>

#include <Common/config.h>
#if USE_PROTOBUF

#include <boost/noncopyable.hpp>
#include <IO/WriteBufferFromString.h>


namespace google
{
namespace protobuf
{
    class Descriptor;
    class FieldDescriptor;
}
}

namespace DB
{
class IAggregateFunction;
using AggregateFunctionPtr = std::shared_ptr<IAggregateFunction>;
using ConstAggregateDataPtr = const char *;


/** Serializes a protobuf, tries to cast types if necessarily.
  */
class ProtobufWriter : private boost::noncopyable
{
public:
    ProtobufWriter(WriteBuffer & out, const google::protobuf::Descriptor * message_type);
    ~ProtobufWriter();

    /// Returns fields of the protobuf schema sorted by their numbers.
    const std::vector<const google::protobuf::FieldDescriptor *> & fieldsInWriteOrder() const;

    /// Should be called when we start writing a new message.
    void newMessage();

    /// Should be called when we start writing a new field.
    /// Returns false if there is no more fields in the message type.
    bool nextField();

    /// Returns the current field of the message type.
    /// The value returned by this function changes after calling nextField() or newMessage().
    const google::protobuf::FieldDescriptor * currentField() const { return current_field; }

    void writeNumber(Int8 value);
    void writeNumber(UInt8 value);
    void writeNumber(Int16 value);
    void writeNumber(UInt16 value);
    void writeNumber(Int32 value);
    void writeNumber(UInt32 value);
    void writeNumber(Int64 value);
    void writeNumber(UInt64 value);
    void writeNumber(UInt128 value);
    void writeNumber(Float32 value);
    void writeNumber(Float64 value);

    void writeString(const StringRef & value);

    void prepareEnumMapping(const std::vector<std::pair<std::string, Int8>> & name_value_pairs);
    void prepareEnumMapping(const std::vector<std::pair<std::string, Int16>> & name_value_pairs);
    void writeEnum(Int8 value);
    void writeEnum(Int16 value);

    void writeUUID(const UUID & value);
    void writeDate(DayNum date);
    void writeDateTime(time_t tm);

    void writeDecimal(Decimal32 decimal, UInt32 scale);
    void writeDecimal(Decimal64 decimal, UInt32 scale);
    void writeDecimal(const Decimal128 & decimal, UInt32 scale);

    void writeAggregateFunction(const AggregateFunctionPtr & function, ConstAggregateDataPtr place);

private:
    void enumerateFieldsInWriteOrder(const google::protobuf::Descriptor * message_type);
    void createConverters();

    void finishCurrentMessage();
    void finishCurrentField();

    class SimpleWriter
    {
    public:
        SimpleWriter(WriteBuffer & out_);
        ~SimpleWriter();

        void newMessage();
        void setCurrentField(UInt32 field_number);
        UInt32 currentFieldNumber() const { return current_field_number; }
        size_t numValues() const { return num_normal_values + num_packed_values; }

        void writeInt32(Int32 value);
        void writeUInt32(UInt32 value);
        void writeSInt32(Int32 value);
        void writeInt64(Int64 value);
        void writeUInt64(UInt64 value);
        void writeSInt64(Int64 value);
        void writeFixed32(UInt32 value);
        void writeSFixed32(Int32 value);
        void writeFloat(float value);
        void writeFixed64(UInt64 value);
        void writeSFixed64(Int64 value);
        void writeDouble(double value);
        void writeString(const StringRef & str);

        void writeInt32IfNonZero(Int32 value);
        void writeUInt32IfNonZero(UInt32 value);
        void writeSInt32IfNonZero(Int32 value);
        void writeInt64IfNonZero(Int64 value);
        void writeUInt64IfNonZero(UInt64 value);
        void writeSInt64IfNonZero(Int64 value);
        void writeFixed32IfNonZero(UInt32 value);
        void writeSFixed32IfNonZero(Int32 value);
        void writeFloatIfNonZero(float value);
        void writeFixed64IfNonZero(UInt64 value);
        void writeSFixed64IfNonZero(Int64 value);
        void writeDoubleIfNonZero(double value);
        void writeStringIfNotEmpty(const StringRef & str);

        void packRepeatedInt32(Int32 value);
        void packRepeatedUInt32(UInt32 value);
        void packRepeatedSInt32(Int32 value);
        void packRepeatedInt64(Int64 value);
        void packRepeatedUInt64(UInt64 value);
        void packRepeatedSInt64(Int64 value);
        void packRepeatedFixed32(UInt32 value);
        void packRepeatedSFixed32(Int32 value);
        void packRepeatedFloat(float value);
        void packRepeatedFixed64(UInt64 value);
        void packRepeatedSFixed64(Int64 value);
        void packRepeatedDouble(double value);

    private:
        void finishCurrentMessage();
        void finishCurrentField();

        enum WireType : UInt32;
        void writeKey(WireType wire_type, WriteBuffer & buf);

        WriteBuffer & out;
        bool were_messages = false;
        WriteBufferFromOwnString message_buffer;
        UInt32 current_field_number = 0;
        size_t num_normal_values = 0;
        size_t num_packed_values = 0;
        WriteBufferFromOwnString repeated_packing_buffer;
    };

    SimpleWriter simple_writer;
    std::vector<const google::protobuf::FieldDescriptor *> fields_in_write_order;
    size_t current_field_index = -1;
    const google::protobuf::FieldDescriptor * current_field = nullptr;

    class Converter;
    class ToStringConverter;
    template <typename T>
    class ToNumberConverter;
    class ToBoolConverter;
    class ToEnumConverter;

    std::vector<std::unique_ptr<Converter>> converters;
    Converter * current_converter = nullptr;
};

}

#else

namespace DB
{
class IAggregateFunction;
using AggregateFunctionPtr = std::shared_ptr<IAggregateFunction>;
using ConstAggregateDataPtr = const char *;

class ProtobufWriter
{
public:
    void writeNumber(Int8 value) {}
    void writeNumber(UInt8 value) {}
    void writeNumber(Int16 value) {}
    void writeNumber(UInt16 value) {}
    void writeNumber(Int32 value) {}
    void writeNumber(UInt32 value) {}
    void writeNumber(Int64 value) {}
    void writeNumber(UInt64 value) {}
    void writeNumber(UInt128 value) {}
    void writeNumber(Float32 value) {}
    void writeNumber(Float64 value) {}
    void writeString(const StringRef & value) {}
    void prepareEnumMapping(const std::vector<std::pair<std::string, Int8>> & name_value_pairs) {}
    void prepareEnumMapping(const std::vector<std::pair<std::string, Int16>> & name_value_pairs) {}
    void writeEnum(Int8 value) {}
    void writeEnum(Int16 value) {}
    void writeUUID(const UUID & value) {}
    void writeDate(DayNum date) {}
    void writeDateTime(time_t tm) {}
    void writeDecimal(Decimal32 decimal, UInt32 scale) {}
    void writeDecimal(Decimal64 decimal, UInt32 scale) {}
    void writeDecimal(const Decimal128 & decimal, UInt32 scale) {}
    void writeAggregateFunction(const AggregateFunctionPtr & function, ConstAggregateDataPtr place) {}
};

}
#endif
