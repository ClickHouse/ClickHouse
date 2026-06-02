#pragma once

#include <Interpreters/InterserverIOHandler.h>
#include <boost/noncopyable.hpp>

namespace DB
{

class StatelessTaskExecutor;

/// Endpoint for serving merge requests.
class StatelessWorkerEndpoint final : public InterserverIOEndpoint, private boost::noncopyable
{
public:
    StatelessWorkerEndpoint();
    ~StatelessWorkerEndpoint() override;

    std::string getId(const std::string & path) const override;
    void processQuery(const HTMLForm & params, ReadBufferPtr body, WriteBuffer & out, HTTPServerResponse & response) override;
    void shutdown();

private:
    const std::string endpoint_name;
    LoggerPtr log;
    std::shared_ptr<StatelessTaskExecutor> task_runner;
};

struct DistributedQueryTaskDescription;

void serializeTask(const DistributedQueryTaskDescription & task_description, WriteBuffer & out);

}
