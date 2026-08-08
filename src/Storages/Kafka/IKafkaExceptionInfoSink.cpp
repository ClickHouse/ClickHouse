#include <Storages/Kafka/IKafkaExceptionInfoSink.h>

#include <cppkafka/error.h>
#include <base/defines.h>

namespace DB
{

void IKafkaExceptionInfoSink::setExceptionInfo(const cppkafka::Error & err)
{
    setExceptionInfo(err.to_string(), /* with_stacktrace = */ false);
}

void IKafkaExceptionInfoSink::setExceptionInfo(const std::string & text, bool with_stacktrace)
{
    UNUSED(text);
    UNUSED(with_stacktrace);
}

IKafkaExceptionInfoSink::~IKafkaExceptionInfoSink() = default;

}
