#pragma once

#include <memory>
#include <cppkafka/cppkafka.h>
#include <Core/Defines.h>
#include <Core/Settings.h>

namespace DB
{
class IKafkaExceptionInfoSink
{
public:
    virtual void setExceptionInfo(const cppkafka::Error & err, bool with_stacktrace)
    {
        UNUSED(err);
        UNUSED(with_stacktrace);
    }

    virtual void setExceptionInfo(const std::string & text, bool with_stacktrace)
    {
        UNUSED(text);
        UNUSED(with_stacktrace);
    }

    virtual ~IKafkaExceptionInfoSink() = default;
};

using IKafkaExceptionInfoSinkPtr = std::shared_ptr<IKafkaExceptionInfoSink>;
using IKafkaExceptionInfoSinkWeakPtr = std::weak_ptr<IKafkaExceptionInfoSink>;

}
