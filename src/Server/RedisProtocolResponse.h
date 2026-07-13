#pragma once

#include <vector>

#include <IO/WriteBuffer.h>
#include <Server/RedisProtocolReaderWriter.h>

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
    explicit ErrorResponse(const String & error_) : error(error_) {}

    void serialize(WriteBuffer & out) final
    {
        Writer writer(out);
        writer.writeError(error);
    }

private:
    const String error;
};

class SimpleStringResponse : public IResponse
{
public:
    explicit SimpleStringResponse(const String & str_) : str(str_) {}

    void serialize(WriteBuffer & out) final
    {
        Writer writer(out);
        writer.writeSimpleString(str);
    }

private:
    const String str;
};

/// An empty string is serialized as Nil, because `joinGet` returns
/// a default value when the key is not found.
class BulkStringResponse : public IResponse
{
public:
    explicit BulkStringResponse(const String & str_) : str(str_) {}

    void serialize(WriteBuffer & out) final
    {
        Writer writer(out);
        if (str.empty())
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
        Writer writer(out);
        writer.writeArray(values.size());
        for (const auto & value : values)
        {
            if (value.empty())
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
