#pragma once

#include <Processors/ISink.h>
#include <Storages/IStorageWriteSession.h>

#include <vector>

namespace DB
{

/// Receives the output of all the streams of an `INSERT` and commits its write sessions when they have finished.
class InsertCommitSink final : public ISink
{
public:
    InsertCommitSink(SharedHeader header, std::vector<StorageWriteSessionPtr> sessions_);

    String getName() const override { return "InsertCommitSink"; }

protected:
    void consume(Chunk chunk) override;
    void onFinish() override;

private:
    std::vector<StorageWriteSessionPtr> sessions;
};

}
