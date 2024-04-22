#pragma once

#include "config.h"

#if USE_CBOR

#    include <cbor.h>
#    include <IO/ReadBuffer.h>


namespace DB
{
class ReadBuffer;

class CBORReader
{
public:
    enum InputSchema
    {
        DEFAULT, // [_ [], [], [], ...]
        JSON_LIKE // [{}, {}, ...]
    };

private:
    InputSchema schema_type = InputSchema::DEFAULT;
    ReadBuffer & buf;
    cbor::input cbor_reader;

    size_t extractBytesCountFromMinorType(unsigned char minor_type);
    uint64_t extractValueFromMinorType(unsigned char minor_type);

    size_t getArraySize(unsigned char minor_type);
    size_t getMapSize(unsigned char minor_type);

    size_t getTypeBytesCount(unsigned char type);
    size_t getIntegerBytesCount(unsigned char minors_type);
    size_t getByteStringBytesCount(unsigned char minor_type);
    size_t getStringBytesCount(unsigned char minor_type);
    size_t getArrayBytesCount(unsigned char minor_type);
    size_t getMapBytesCount(unsigned char minor_type);
    size_t getTagBytesCount(unsigned char minor_type);
    size_t getSpecialBytesCount(unsigned char minor_type);


public:
    CBORReader(ReadBuffer & in_);
    ~CBORReader();

    void readAndCheckPrefix();
    bool readRow(size_t expected_size, cbor::listener & listener);
};
}

#endif
