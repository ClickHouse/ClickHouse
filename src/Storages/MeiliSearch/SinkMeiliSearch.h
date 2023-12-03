#pragma once

#include <Core/ExternalResultDescription.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/MeiliSearch/MeiliSearchConnection.h>

namespace DB
{
class SinkMeiliSearch : public SinkToStorage
{
public:
    SinkMeiliSearch(const MeiliSearchConfiguration & config_, const Block & sample_block_, ContextPtr local_context_);

    String getName() const override { return "SinkMeiliSearch"; }

    void consume(Chunk chunk) override;

    void writeBlockData(const Block & block) const;

private:
    MeiliSearchConnection connection;
    ContextPtr local_context;
    Block sample_block;
};

}
