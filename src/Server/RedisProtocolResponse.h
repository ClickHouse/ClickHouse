#pragma once

#include <optional>
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

/// An unset optional is serialized as Nil (the key or field was not found).
class BulkStringResponse : public IResponse
{
public:
    explicit BulkStringResponse(std::optional<String> str_) : str(std::move(str_)) {}

    void serialize(WriteBuffer & out) final
    {
        Writer writer(out);
        if (!str)
        {
            writer.writeNil();
            return;
        }
        writer.writeBulkString(*str);
    }

private:
    const std::optional<String> str;
};

class ArrayResponse : public IResponse
{
public:
    explicit ArrayResponse(const std::vector<std::optional<String>> & values_) : values(values_) {}

    void serialize(WriteBuffer & out) final
    {
        Writer writer(out);
        writer.writeArray(values.size());
        for (const auto & value : values)
        {
            if (!value)
            {
                writer.writeNil();
                continue;
            }
            writer.writeBulkString(*value);
        }
    }

private:
    const std::vector<std::optional<String>> & values;
};

}

}
