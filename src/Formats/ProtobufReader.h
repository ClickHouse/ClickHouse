#pragma once

#include <common/DayNum.h>
#include <Common/PODArray.h>
#include <Common/UInt128.h>
#include <Core/UUID.h>

#if !defined(ARCADIA_BUILD)
#    include "config_formats.h"
#endif

#if USE_PROTOBUF
#    include <memory>
#    include <IO/ReadBuffer.h>
#    include <boost/noncopyable.hpp>
#    include "ProtobufColumnMatcher.h"

namespace google
{
namespace protobuf
{
    class Descriptor;
}
}

namespace DB
{
class Arena;
class IAggregateFunction;
class ReadBuffer;
using AggregateDataPtr = char *;
using AggregateFunctionPtr = std::shared_ptr<IAggregateFunction>;


/** Deserializes a protobuf, tries to cast data types if necessarily.
  */
class ProtobufReader : private boost::noncopyable
{
public:
    ProtobufReader(ReadBuffer & in_, const google::protobuf::Descriptor * message_type, const std::vector<String> & column_names, const bool use_length_delimiters_);
    ~ProtobufReader();

    /// Should be called when we start reading a new message.
    bool startMessage();

    /// Ends reading a message.
    void endMessage(bool ignore_errors = false);

    /// Reads the column index.
    /// The function returns false if there are no more columns to read (call endMessage() in this case).
    bool readColumnIndex(size_t & column_index);

    /// Reads a value which should be put to column at index received with readColumnIndex().
    /// The function returns false if there are no more values to read now (call readColumnIndex() in this case).
    bool readNumber(Int8 & value) { return current_converter->readInt8(value); }
    bool readNumber(UInt8 & value) { return current_converter->readUInt8(value); }
    bool readNumber(Int16 & value) { return current_converter->readInt16(value); }
    bool readNumber(UInt16 & value) { return current_converter->readUInt16(value); }
    bool readNumber(Int32 & value) { return current_converter->readInt32(value); }
    bool readNumber(UInt32 & value) { return current_converter->readUInt32(value); }
    bool readNumber(Int64 & value) { return current_converter->readInt64(value); }
    bool readNumber(UInt64 & value) { return current_converter->readUInt64(value); }
    bool readNumber(Int128 & value) { return current_converter->readInt128(value); }
    bool readNumber(UInt128 & value) { return current_converter->readUInt128(value); }
    bool readNumber(Int256 & value) { return current_converter->readInt256(value); }
    bool readNumber(UInt256 & value) { return current_converter->readUInt256(value); }
    bool readNumber(Float32 & value) { return current_converter->readFloat32(value); }
    bool readNumber(Float64 & value) { return current_converter->readFloat64(value); }

    bool readStringInto(PaddedPODArray<UInt8> & str) { return current_converter->readStringInto(str); }

    void prepareEnumMapping(const std::vector<std::pair<std::string, Int8>> & name_value_pairs) { current_converter->prepareEnumMapping8(name_value_pairs); }
    void prepareEnumMapping(const std::vector<std::pair<std::string, Int16>> & name_value_pairs) { current_converter->prepareEnumMapping16(name_value_pairs); }
    bool readEnum(Int8 & value) { return current_converter->readEnum8(value); }
    bool readEnum(Int16 & value) { return current_converter->readEnum16(value); }

    bool readUUID(UUID & uuid) { return current_converter->readUUID(uuid); }
    bool readDate(DayNum & date) { return current_converter->readDate(date); }
    bool readDateTime(time_t & tm) { return current_converter->readDateTime(tm); }
    bool readDateTime64(DateTime64 & tm, UInt32 scale) { return current_converter->readDateTime64(tm, scale); }

    bool readDecimal(Decimal32 & decimal, UInt32 precision, UInt32 scale) { return current_converter->readDecimal32(decimal, precision, scale); }
    bool readDecimal(Decimal64 & decimal, UInt32 precision, UInt32 scale) { return current_converter->readDecimal64(decimal, precision, scale); }
    bool readDecimal(Decimal128 & decimal, UInt32 precision, UInt32 scale) { return current_converter->readDecimal128(decimal, precision, scale); }
    bool readDecimal(Decimal256 & decimal, UInt32 precision, UInt32 scale) { return current_converter->readDecimal256(decimal, precision, scale); }

    bool readAggregateFunction(const AggregateFunctionPtr & function, AggregateDataPtr place, Arena & arena) { return current_converter->readAggregateFunction(function, place, arena); }

    /// Call it after calling one of the read*() function to determine if there are more values available for reading.
    bool ALWAYS_INLINE canReadMoreValues() const { return simple_reader.canReadMoreValues(); }

private:
    class SimpleReader
    {
    public:
        SimpleReader(ReadBuffer & in_, const bool use_length_delimiters_);
        bool startMessage();
        void endMessage(bool ignore_errors);
        void startNestedMessage();
        void endNestedMessage();
        bool readFieldNumber(UInt32 & field_number);
        bool readInt(Int64 & value);
        bool readSInt(Int64 & value);
        bool readUInt(UInt64 & value);
        template<typename T> bool readFixed(T & value);
        bool readStringInto(PaddedPODArray<UInt8> & str);

        bool ALWAYS_INLINE canReadMoreValues() const { return cursor < field_end; }

    private:
        void readBinary(void * data, size_t size);
        void ignore(UInt64 num_bytes);
        void moveCursorBackward(UInt64 num_bytes);

        UInt64 ALWAYS_INLINE readVarint()
        {
            char c;
            in.readStrict(c);
            UInt64 first_byte = static_cast<UInt8>(c);
            ++cursor;
            if (likely(!(c & 0x80)))
                return first_byte;
            return continueReadingVarint(first_byte);
        }

        UInt64 continueReadingVarint(UInt64 first_byte);
        void ignoreVarint();
        void ignoreGroup();
        [[noreturn]] void throwUnknownFormat() const;

        ReadBuffer & in;
        Int64 cursor;
        size_t current_message_level;
        Int64 current_message_end;
        std::vector<Int64> parent_message_ends;
        Int64 field_end;
        Int64 last_string_pos;
        const bool use_length_delimiters;
    };

    class IConverter
    {
    public:
       virtual ~IConverter() = default;
       virtual bool readStringInto(PaddedPODArray<UInt8> &) = 0;
       virtual bool readInt8(Int8&) = 0;
       virtual bool readUInt8(UInt8 &) = 0;
       virtual bool readInt16(Int16 &) = 0;
       virtual bool readUInt16(UInt16 &) = 0;
       virtual bool readInt32(Int32 &) = 0;
       virtual bool readUInt32(UInt32 &) = 0;
       virtual bool readInt64(Int64 &) = 0;
       virtual bool readUInt64(UInt64 &) = 0;
       virtual bool readInt128(Int128 &) = 0;
       virtual bool readUInt128(UInt128 &) = 0;

       virtual bool readInt256(Int256 &) = 0;
       virtual bool readUInt256(UInt256 &) = 0;

       virtual bool readFloat32(Float32 &) = 0;
       virtual bool readFloat64(Float64 &) = 0;
       virtual void prepareEnumMapping8(const std::vector<std::pair<std::string, Int8>> &) = 0;
       virtual void prepareEnumMapping16(const std::vector<std::pair<std::string, Int16>> &) = 0;
       virtual bool readEnum8(Int8 &) = 0;
       virtual bool readEnum16(Int16 &) = 0;
       virtual bool readUUID(UUID &) = 0;
       virtual bool readDate(DayNum &) = 0;
       virtual bool readDateTime(time_t &) = 0;
       virtual bool readDateTime64(DateTime64 &, UInt32) = 0;
       virtual bool readDecimal32(Decimal32 &, UInt32, UInt32) = 0;
       virtual bool readDecimal64(Decimal64 &, UInt32, UInt32) = 0;
       virtual bool readDecimal128(Decimal128 &, UInt32, UInt32) = 0;
       virtual bool readDecimal256(Decimal256 &, UInt32, UInt32) = 0;
       virtual bool readAggregateFunction(const AggregateFunctionPtr &, AggregateDataPtr, Arena &) = 0;
    };

    class ConverterBaseImpl;
    class ConverterFromString;
    template<int field_type_id, typename FromType> class ConverterFromNumber;
    class ConverterFromBool;
    class ConverterFromEnum;

    struct ColumnMatcherTraits
    {
        struct FieldData
        {
            std::unique_ptr<IConverter> converter;
        };
        struct MessageData
        {
            std::unordered_map<UInt32, const ProtobufColumnMatcher::Field<ColumnMatcherTraits>*> field_number_to_field_map;
        };
    };
    using Message = ProtobufColumnMatcher::Message<ColumnMatcherTraits>;
    using Field = ProtobufColumnMatcher::Field<ColumnMatcherTraits>;

    void setTraitsDataAfterMatchingColumns(Message * message);

    template <int field_type_id>
    std::unique_ptr<IConverter> createConverter(const google::protobuf::FieldDescriptor * field);

    SimpleReader simple_reader;
    std::unique_ptr<Message> root_message;
    Message* current_message = nullptr;
    size_t current_field_index = 0;
    IConverter* current_converter = nullptr;
};

}

#else

namespace DB
{
class Arena;
class IAggregateFunction;
class ReadBuffer;
using AggregateDataPtr = char *;
using AggregateFunctionPtr = std::shared_ptr<IAggregateFunction>;

class ProtobufReader
{
public:
    bool startMessage() { return false; }
    void endMessage() {}
    bool readColumnIndex(size_t &) { return false; }
    bool readNumber(Int8 &) { return false; }
    bool readNumber(UInt8 &) { return false; }
    bool readNumber(Int16 &) { return false; }
    bool readNumber(UInt16 &) { return false; }
    bool readNumber(Int32 &) { return false; }
    bool readNumber(UInt32 &) { return false; }
    bool readNumber(Int64 &) { return false; }
    bool readNumber(UInt64 &) { return false; }
    bool readNumber(Int128 &) { return false; }
    bool readNumber(UInt128 &) { return false; }
    bool readNumber(Int256 &) { return false; }
    bool readNumber(UInt256 &) { return false; }
    bool readNumber(Float32 &) { return false; }
    bool readNumber(Float64 &) { return false; }
    bool readStringInto(PaddedPODArray<UInt8> &) { return false; }
    void prepareEnumMapping(const std::vector<std::pair<std::string, Int8>> &) {}
    void prepareEnumMapping(const std::vector<std::pair<std::string, Int16>> &) {}
    bool readEnum(Int8 &) { return false; }
    bool readEnum(Int16 &) { return false; }
    bool readUUID(UUID &) { return false; }
    bool readDate(DayNum &) { return false; }
    bool readDateTime(time_t &) { return false; }
    bool readDateTime64(DateTime64 & /*tm*/, UInt32 /*scale*/) { return false; }
    bool readDecimal(Decimal32 &, UInt32, UInt32) { return false; }
    bool readDecimal(Decimal64 &, UInt32, UInt32) { return false; }
    bool readDecimal(Decimal128 &, UInt32, UInt32) { return false; }
    bool readDecimal(Decimal256 &, UInt32, UInt32) { return false; }
    bool readAggregateFunction(const AggregateFunctionPtr &, AggregateDataPtr, Arena &) { return false; }
    bool canReadMoreValues() const { return false; }
};

}
#endif
