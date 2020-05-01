#pragma once

#include <DataTypes/DataTypesNumber.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/PushingToViewsBlockOutputStream.h>
#include <Storages/LiveView/StorageLiveView.h>


namespace DB
{

class LiveViewBlockOutputStream : public IBlockOutputStream
{
public:
    explicit LiveViewBlockOutputStream(StorageLiveView & storage_, const Context & context) : storage(storage_)
    {
        target_table_stream = storage.tryGetTargetTableOutputStream(context);
    }

    void writePrefix() override
    {
        new_blocks = std::make_shared<Blocks>();
        new_blocks_metadata = std::make_shared<BlocksMetadata>();
        new_hash = std::make_shared<SipHash>();
        if (target_table_stream)
            target_table_stream->writePrefix();
    }

    void writeSuffix() override
    {
        UInt128 key;
        String key_str;

        new_hash->get128(key.low, key.high);
        key_str = key.toHexString();

        std::lock_guard lock(storage.mutex);

        if (storage.getBlocksHashKey() != key_str)
        {
            new_blocks_metadata->hash = key_str;
            new_blocks_metadata->version = storage.getBlocksVersion() + 1;

            for (auto & block : *new_blocks)
            {
                block.insert({DataTypeUInt64().createColumnConst(
                    block.rows(), new_blocks_metadata->version)->convertToFullColumnIfConst(),
                    std::make_shared<DataTypeUInt64>(),
                    "_version"});
                if (target_table_stream)
                    target_table_stream->write(block);
            }

            (*storage.blocks_ptr) = new_blocks;
            (*storage.blocks_metadata_ptr) = new_blocks_metadata;

            storage.condition.notify_all();
        }

        new_blocks.reset();
        new_blocks_metadata.reset();
        new_hash.reset();

        if (target_table_stream)
            target_table_stream->writeSuffix();
    }

    void write(const Block & block) override
    {
        new_blocks->push_back(block);
        block.updateHash(*new_hash);
    }

    void flush() override
    {
        if (target_table_stream)
            target_table_stream->flush();
    }

    Block getHeader() const override { return storage.getHeader(); }

private:
    using SipHashPtr = std::shared_ptr<SipHash>;

    BlocksPtr new_blocks;
    BlocksMetadataPtr new_blocks_metadata;
    SipHashPtr new_hash;
    StorageLiveView & storage;
    BlockOutputStreamPtr target_table_stream = nullptr;
};

}
