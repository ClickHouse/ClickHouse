#pragma once

#include <memory>

namespace cppkafka
{
class Error;
}
namespace DB
{
class IKafkaExceptionInfoSink
{
public:
    /// A librdkafka-originated error carries no useful ClickHouse stack trace - the trace only shows the
    /// fixed callback path - so this overload never stores one. `system.kafka_consumers.exceptions` is a
    /// 10-entry ring buffer, and a stored trace pushes the broker message off it.
    virtual void setExceptionInfo(const cppkafka::Error & err);
    virtual void setExceptionInfo(const std::string & text, bool with_stacktrace);
    virtual ~IKafkaExceptionInfoSink();
};

using IKafkaExceptionInfoSinkPtr = std::shared_ptr<IKafkaExceptionInfoSink>;
using IKafkaExceptionInfoSinkWeakPtr = std::weak_ptr<IKafkaExceptionInfoSink>;

}
