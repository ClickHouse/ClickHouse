#pragma once

#include <Server/Kafka/KafkaProtocol.h>
#include <Common/Exception.h>


namespace DB::Kafka
{

/// An exception that carries the Kafka error code to report to the client.
class Error : public Exception
{
public:
    template <typename... Args>
    Error(ErrorCode kafka_code_, int code, FormatStringHelper<Args...> fmt, Args &&... args)
        : Exception(code, std::move(fmt), std::forward<Args>(args)...), kafka_code(kafka_code_)
    {
    }

    [[nodiscard]] ErrorCode kafkaCode() const { return kafka_code; }

    Exception * clone() const override { return new Error(*this); }
    void rethrow() const override { throw *this; } // NOLINT

private:
    ErrorCode kafka_code;
};

}
