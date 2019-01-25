#pragma once

#include <Core/Types.h>
#include <boost/noncopyable.hpp>
#include "IO/WriteBufferFromString.h"


namespace DB
{
/** Utility class to serialize protobufs.
  * Knows nothing about protobuf schemas, just provides useful functions to serialize data.
  * This class is written following the documentation: https://developers.google.com/protocol-buffers/docs/encoding
  */
class ProtobufSimpleWriter : private boost::noncopyable
{
public:
    ProtobufSimpleWriter(WriteBuffer & out_);
    ~ProtobufSimpleWriter();

    /// Should be called when we start writing a new message.
    void newMessage();

    /// Should be called when we start writing a new field.
    /// A passed 'field_number' should be positive and greater than any previous 'field_number'.
    void setCurrentField(UInt32 field_number);
    UInt32 currentFieldNumber() const { return current_field_number; }

    /// Returns number of values added to the current field.
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

}
