#pragma once
#include <Processors/ISink.h>

namespace DB
{

class IBlockOutputStream;
using BlockOutputStreamPtr = std::shared_ptr<IBlockOutputStream>;

/// Sink which writes data to IBlockOutputStream.
/// It's a temporary wrapper.
class SinkToOutputStream : public ISink
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
