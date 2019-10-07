#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/Context.h>

#include <Storages/Kafka/StorageKafka.h>

namespace DB
{

class KafkaBlockInputStream : public IBlockInputStream
{
public:
    KafkaBlockInputStream(StorageKafka & storage_, const Context & context_, const Names & columns, size_t max_block_size_);
    ~KafkaBlockInputStream() override;

    String getName() const override { return storage.getName(); }
    Block getHeader() const override;

    void readPrefixImpl() override;
    Block readImpl() override;
    void readSuffixImpl() override;

private:
    StorageKafka & storage;
    Context context;
    Names column_names;
    UInt64 max_block_size;

    BufferPtr buffer;
    MutableColumns virtual_columns;
    bool broken = true, claimed = false;
};

}
