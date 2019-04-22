#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/Context.h>

#include <Storages/Kafka/StorageKafka.h>

namespace DB
{

class KafkaBlockInputStream : public IBlockInputStream
{
public:
    KafkaBlockInputStream(StorageKafka & storage_, const Context & context_, const String & schema, size_t max_block_size_);
    ~KafkaBlockInputStream() override;

    String getName() const override { return storage.getName(); }
    Block readImpl() override { return children.back()->read(); }
    Block getHeader() const override { return storage.getSampleBlock(); }

    void readPrefixImpl() override;
    void readSuffixImpl() override;

private:
    StorageKafka & storage;
    Context context;
    UInt64 max_block_size;

    BufferPtr buffer;
    bool broken = true, claimed = false;
};

} // namespace DB
