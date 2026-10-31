#pragma once

#include <pulsar/Logger.h>

#include <Common/Logger.h>

namespace DB
{

/// Routes the Apache Pulsar client library logs into the server logging.
/// Without it the client uses its default `ConsoleLoggerFactory`, which writes to `std::cout`
/// from the client's internal threads without synchronization (a data race on the stream
/// reported by TSan, and interleaved garbage in stdout in the best case).
class PulsarLoggerFactory : public pulsar::LoggerFactory
{
public:
    pulsar::Logger * getLogger(const std::string & file_name) override;
};

}
