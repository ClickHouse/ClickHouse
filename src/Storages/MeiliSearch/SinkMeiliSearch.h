#include <Processors/Sinks/SinkToStorage.h>
#include <Core/ExternalResultDescription.h>
#include <Storages/MeiliSearch/MeiliSearchConnection.h>

namespace DB
{

class SinkMeiliSearch : public SinkToStorage
{
public:
    SinkMeiliSearch(
        const MeiliSearchConfiguration& config_,
        const Block & sample_block_,
        UInt64 max_block_size_);

    String getName() const override { return "SinkMeiliSearch"; }

    void consume(Chunk chunk) override;

    void writeBlockData(const Block & block);

    Blocks splitBlocks(const Block & block, const size_t & max_rows) const;

private:

    String getOneElement(const Block& block, int ind);

    MeiliSearchConnection connection;
    const UInt64 max_block_size;
    Block sample_block;

};

}
