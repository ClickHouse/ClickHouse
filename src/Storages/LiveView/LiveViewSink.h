#pragma once

#include <DataTypes/DataTypesNumber.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/LiveView/StorageLiveView.h>
#include <base/hex.h>


namespace DB
{

class LiveViewSink : public SinkToStorage
{
    /// _version column is added manually in sink.
    static Block updateHeader(Block block)
    {
        block.erase("_version");
        return block;
    }

public:
    explicit LiveViewSink(StorageLiveView & storage_) : SinkToStorage(updateHeader(storage_.getHeader())), storage(storage_) {}

    String getName() const override { return "LiveViewSink"; }

    void onStart() override
    {
        new_blocks = std::make_shared<Blocks>();
        new_blocks_metadata = std::make_shared<BlocksMetadata>();
        new_hash = std::make_shared<SipHash>();
    }

    void onFinish() override
    {
        const auto key = new_hash->get128();
        const auto key_str = getHexUIntLowercase(key);

        std::lock_guard lock(storage.mutex);

        if (storage.getBlocksHashKey(lock) != key_str)
        {
            new_blocks_metadata->hash = key_str;
            new_blocks_metadata->version = storage.getBlocksVersion(lock) + 1;
            new_blocks_metadata->time = std::chrono::system_clock::now();

            for (auto & block : *new_blocks)
            {
                block.insert({DataTypeUInt64().createColumnConst(
                    block.rows(), new_blocks_metadata->version)->convertToFullColumnIfConst(),
                    std::make_shared<DataTypeUInt64>(),
                    "_version"});
            }

            (*storage.blocks_ptr) = new_blocks;
            (*storage.blocks_metadata_ptr) = new_blocks_metadata;

            storage.condition.notify_all();
        }
        else
        {
            // only update blocks time
            new_blocks_metadata->hash = storage.getBlocksHashKey(lock);
            new_blocks_metadata->version = storage.getBlocksVersion(lock);
            new_blocks_metadata->time = std::chrono::system_clock::now();

            (*storage.blocks_metadata_ptr) = new_blocks_metadata;
        }

        new_blocks.reset();
        new_blocks_metadata.reset();
        new_hash.reset();
    }

    void consume(Chunk & chunk) override
    {
        auto block = getHeader().cloneWithColumns(chunk.getColumns());
        block.updateHash(*new_hash);
        new_blocks->push_back(std::move(block));
    }

private:
    using SipHashPtr = std::shared_ptr<SipHash>;

    BlocksPtr new_blocks;
    BlocksMetadataPtr new_blocks_metadata;
    SipHashPtr new_hash;
    StorageLiveView & storage;
};

}
