#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Interpreters/Context.h>
#include <Storages/Kafka/StorageKafka.h>

namespace DB
{

class KafkaBlockOutputStream : public IBlockOutputStream
{
public:
    explicit KafkaBlockOutputStream(StorageKafka & storage_, const Context & context_);

    Block getHeader() const override;

    void writePrefix() override;
    void write(const Block & block) override;
    void writeSuffix() override;

    void flush() override;

private:
    StorageKafka & storage;
    Context context;
    ProducerBufferPtr buffer;
    BlockOutputStreamPtr child;
};

}
