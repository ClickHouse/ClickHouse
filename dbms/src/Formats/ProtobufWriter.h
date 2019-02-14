#pragma once

#include <Core/UUID.h>
#include <Formats/ProtobufSimpleWriter.h>
#include <boost/noncopyable.hpp>
#include <Common/PODArray.h>
#include <Common/UInt128.h>
#include <common/DayNum.h>
#include <Common/config.h>


namespace google
{
namespace protobuf
{
    class Descriptor;
    class FieldDescriptor;
}
}

#if USE_PROTOBUF
#   define EMPTY_DEF
#   define EMPTY_DEF_RET(a)
#else
#   define EMPTY_DEF {}
#   define EMPTY_DEF_RET(a) {return a;}
#   pragma GCC diagnostic push
#   pragma GCC diagnostic ignored "-Wunused-parameter"
#   pragma GCC diagnostic ignored "-Wextra-semi"
#endif


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
    ProtobufWriter(WriteBuffer & out, const google::protobuf::Descriptor * message_type) EMPTY_DEF;
    ~ProtobufWriter() EMPTY_DEF;

    /// Returns fields of the protobuf schema sorted by their numbers.
    const std::vector<const google::protobuf::FieldDescriptor *> & fieldsInWriteOrder() const;

    /// Should be called when we start writing a new message.
    void newMessage() EMPTY_DEF;

    /// Should be called when we start writing a new field.
    /// Returns false if there is no more fields in the message type.
    bool nextField() EMPTY_DEF_RET(false);

    /// Returns the current field of the message type.
    /// The value returned by this function changes after calling nextField() or newMessage().
#if USE_PROTOBUF
    const google::protobuf::FieldDescriptor * currentField() const { return current_field; }
#endif

    void writeNumber(Int8 value) EMPTY_DEF;
    void writeNumber(UInt8 value) EMPTY_DEF;
    void writeNumber(Int16 value) EMPTY_DEF;
    void writeNumber(UInt16 value) EMPTY_DEF;
    void writeNumber(Int32 value) EMPTY_DEF;
    void writeNumber(UInt32 value) EMPTY_DEF;
    void writeNumber(Int64 value) EMPTY_DEF;
    void writeNumber(UInt64 value) EMPTY_DEF;
    void writeNumber(UInt128 value) EMPTY_DEF;
    void writeNumber(Float32 value) EMPTY_DEF;
    void writeNumber(Float64 value) EMPTY_DEF;

    void writeString(const StringRef & value) EMPTY_DEF;

    void prepareEnumMapping(const std::vector<std::pair<std::string, Int8>> & name_value_pairs) EMPTY_DEF;
    void prepareEnumMapping(const std::vector<std::pair<std::string, Int16>> & name_value_pairs) EMPTY_DEF;
    void writeEnum(Int8 value) EMPTY_DEF;
    void writeEnum(Int16 value) EMPTY_DEF;

    void writeUUID(const UUID & value) EMPTY_DEF;
    void writeDate(DayNum date) EMPTY_DEF;
    void writeDateTime(time_t tm) EMPTY_DEF;

    void writeDecimal(Decimal32 decimal, UInt32 scale) EMPTY_DEF;
    void writeDecimal(Decimal64 decimal, UInt32 scale) EMPTY_DEF;
    void writeDecimal(const Decimal128 & decimal, UInt32 scale) EMPTY_DEF;

    void writeAggregateFunction(const AggregateFunctionPtr & function, ConstAggregateDataPtr place) EMPTY_DEF;

private:
#if USE_PROTOBUF

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

#endif
};

}

#if !USE_PROTOBUF
#   undef EMPTY_DEF
#   undef EMPTY_DEF_RET
#   pragma GCC diagnostic pop
#endif
