#pragma once

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <iostream>

#include "Common/Exception.h"
#include "Common/logger_useful.h"
#include "base/types.h"

namespace DB
{
    namespace ErrorCodes
    {
        extern const int UNEXPECTED_PACKET_FROM_CLIENT;
        extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    }

    namespace RedisProtocol
    {
        enum class DataType : char
        {
            SIMPLE_STRING = '+',
            ERROR = '-',
            INTEGER = ':',
            BULK_STRING = '$',
            ARRAY = '*',
        };

        class Reader
        {
        public:
            explicit Reader(ReadBuffer* buf_) : buf(buf_) {}

            DataType readType()
            {
                char byte;
                if (!buf->read(byte))
                {
                    throw Exception(
                        ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED,
                        "Can't read redis struct type"
                    );
                }
                return static_cast<DataType>(byte);
            }

            Int64 readInteger()
            {
                Int64 num;
                readIntTextImpl(num, *buf);
                skipToCRLFOrEOF(*buf);
                return num;
            }

            String readBulkString()
            {
                auto type = readType();
                if (type != DataType::BULK_STRING)
                {
                    throw Exception(
                        ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT,
                        "Tried to read {} type as Bulk string", type
                    );
                }

                auto size = readInteger();
                String s;
                s.reserve(size);
                readStringUntilCR(s, *buf);
                skipToCRLFOrEOF(*buf);
                return s;
            }
        private:
            ReadBuffer* buf;
        };

        class Writer
        {
        public:
            explicit Writer(WriteBuffer * buf_)
                : buf(buf_)
            {}

            void writeSimpleString(const String & s)
            {
                writeDataType(DataType::SIMPLE_STRING);
                writeString(s, *buf);
                writeCRLF();
            }

            void writeError(const String & s)
            {
                writeDataType(DataType::ERROR);
                writeString("ERR ", *buf);
                writeString(s, *buf);
                writeCRLF();
            }

            void writeInteger(Int64 num)
            {
                writeDataType(DataType::INTEGER);
                writeString(std::to_string(num), *buf);
                writeCRLF();
            }

            void writeBulkString(const String & s)
            {
                writeDataType(DataType::BULK_STRING);
                writeString(std::to_string(s.size()), *buf);
                writeCRLF();
                writeString(s, *buf);
                writeCRLF();
            }

            void writeArray(Int64 num)
            {
                writeDataType(DataType::ARRAY);
                writeString(std::to_string(num), *buf);
                writeCRLF();
            }

            void writeNil()
            {
                writeDataType(DataType::BULK_STRING);
                writeString("-1", *buf);
                writeCRLF();
            }

        private:
            void writeDataType(DataType type)
            {
                buf->write(static_cast<char>(type));
            }

            void writeCRLF()
            {
                const char * crlf = "\r\n";
                buf->write(crlf, 2);
            }

            WriteBuffer * buf;
        };
    }
}
