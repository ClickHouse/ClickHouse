#pragma once
#include <Processors/Sinks/SinkToStorage.h>

namespace DB
{

class IBlockOutputStream;
using BlockOutputStreamPtr = std::shared_ptr<IBlockOutputStream>;

/// Sink which writes data to IBlockOutputStream.
/// It's a temporary wrapper.
class SinkToOutputStream : public SinkToStorage
{
public:
    explicit SinkToOutputStream(BlockOutputStreamPtr stream);

    String getName() const override { return "SinkToOutputStream"; }

protected:
    void consume(Chunk chunk) override;
    void onFinish() override;

private:
    BlockOutputStreamPtr stream;
};

}
