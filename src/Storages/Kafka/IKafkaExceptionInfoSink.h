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
    virtual void setExceptionInfo(const cppkafka::Error & err, bool with_stacktrace);
    virtual void setExceptionInfo(const std::string & text, bool with_stacktrace);
    virtual ~IKafkaExceptionInfoSink();
};

using IKafkaExceptionInfoSinkPtr = std::shared_ptr<IKafkaExceptionInfoSink>;
using IKafkaExceptionInfoSinkWeakPtr = std::weak_ptr<IKafkaExceptionInfoSink>;

}
