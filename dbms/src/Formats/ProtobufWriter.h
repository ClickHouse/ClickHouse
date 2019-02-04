#pragma once

#include <Core/UUID.h>
#include <Formats/ProtobufSimpleWriter.h>
#include <boost/noncopyable.hpp>
#include <Common/PODArray.h>
#include <Common/UInt128.h>
#include <common/DayNum.h>


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

    ProtobufSimpleWriter simple_writer;
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
