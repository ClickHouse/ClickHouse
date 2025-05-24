#pragma once

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
            const String & error;
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
            const String & str;
        };
    }
}
