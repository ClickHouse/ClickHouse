#pragma once

#include <Interpreters/InterserverIOHandler.h>
#include <boost/noncopyable.hpp>

#include <memory>

namespace DB
{

class StatelessTaskExecutor;

/// Serves the task control requests of stateless worker tasks over interserver HTTP.
class StatelessWorkerEndpoint final : public InterserverIOEndpoint, private boost::noncopyable
{
public:
    /// The executor is shared with `StatelessWorkerControlServer`, which shuts it down.
    explicit StatelessWorkerEndpoint(std::shared_ptr<StatelessTaskExecutor> task_runner_);

    std::string getId(const std::string & path) const override;
    void processQuery(const HTMLForm & params, ReadBufferPtr body, WriteBuffer & out, HTTPServerResponse & response) override;

private:
    const std::string endpoint_name;
    LoggerPtr log;
    std::shared_ptr<StatelessTaskExecutor> task_runner;
};

}
