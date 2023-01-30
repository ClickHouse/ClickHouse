#pragma once
#include <Processors/IProcessor.h>

namespace DB
{

class RemoteQueryExecutor;
using RemoteQueryExecutorPtr = std::shared_ptr<RemoteQueryExecutor>;

/// A tiny class which is used for reading with multiple replicas in parallel.
/// Motivation is that we don't have a full control on how
/// processors are scheduled across threads and there could be a situation
/// when all available threads will read from local replica and will just
/// forget about remote replicas existence. That is not what we want.
/// For parallel replicas we have to constantly answer to incoming requests
/// with a set of marks to read.
/// With the help of this class, we explicitly connect a "local" source with
/// all the remote ones. And thus achieve fairness somehow.
class ReadFromMergeTreeDependencyTransform : public IProcessor
{
public:
    explicit ReadFromMergeTreeDependencyTransform(const Block & header);

    String getName() const override { return "ReadFromMergeTreeDependency"; }
    Status prepare() override;

    InputPort & getInputPort() { assert(data_port); return *data_port; }
    InputPort & getDependencyPort() { assert(dependency_port); return *dependency_port; }
    OutputPort & getOutputPort() { return outputs.front(); }

    void connectToScheduler(OutputPort & output_port);
private:
    bool has_data{false};
    Chunk chunk;

    InputPort * data_port{nullptr};
    InputPort * dependency_port{nullptr};

    Status prepareGenerate();
    Status prepareConsume();
};


}
