#include <Core/ExternalResultDescription.h>
#include <Interpreters/Context.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/MeiliSearch/MeiliSearchConnection.h>
#include "Interpreters/Context_fwd.h"


namespace DB
{
class SinkMeiliSearch : public SinkToStorage
{
public:
    SinkMeiliSearch(
        const MeiliSearchConfiguration & config_, const Block & sample_block_, ContextPtr local_context_, UInt64 max_block_size_);

    String getName() const override { return "SinkMeiliSearch"; }

    void consume(Chunk chunk) override;

    void writeBlockData(const Block & block) const;

    Blocks splitBlocks(const Block & block, const size_t & max_rows) const;

private:
    MeiliSearchConnection connection;
    ContextPtr local_context;
    const UInt64 max_block_size;
    Block sample_block;
};

}
