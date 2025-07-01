#pragma once

#include <optional>
#include "IO/WriteBuffer.h"
#include "RedisProtocolReaderWriter.h"

namespace DB
{
    namespace RedisProtocol
    {
        class IResponse
        {
        public:
            virtual void serialize(WriteBuffer & out) = 0;

            virtual ~IResponse() = default;
        };

        class ErrorResponse : public IResponse
        {
        public:
            explicit ErrorResponse(const String & err) : error(err) {}

            void serialize(WriteBuffer & out) final
            {
                Writer writer(&out);
                writer.writeError(error);
            }
        private:
            const String error;
        };

        class NilResponse : public IResponse
        {
            public:
                void serialize(WriteBuffer & out) final
                {
                    Writer writer(&out);
                    writer.writeInteger(-1);
                }
        };

        class SimpleStringResponse : public IResponse
        {
        public:
            explicit SimpleStringResponse(const String & str_) : str(str_) {}

            void serialize(WriteBuffer & out) final
            {
                Writer writer(&out);
                writer.writeSimpleString(str);
            }

        private:
            const String str;
        };

        class BulkStringResponse : public IResponse
        {
        public:
            explicit BulkStringResponse(const String & str_) : str(str_) {}

            void serialize(WriteBuffer & out) final
            {
                Writer writer(&out);
                if (str.size() == 0)
                {
                    writer.writeNil();
                    return;
                }
                writer.writeBulkString(str);
            }
        private:
            const String str;
        };

        class ArrayResponse : public IResponse
        {
        public:
            explicit ArrayResponse(const std::vector<String> & values_) : values(values_) {}

            void serialize(WriteBuffer & out) final
            {
                Writer writer(&out);
                writer.writeArray(values.size());
                for (const auto & value : values)
                {
                    if (value.size() == 0)
                    {
                        writer.writeNil();
                        continue;
                    }
                    writer.writeBulkString(value);
                }
            }

        private:
            const std::vector<String> & values;
        };
    }
}
