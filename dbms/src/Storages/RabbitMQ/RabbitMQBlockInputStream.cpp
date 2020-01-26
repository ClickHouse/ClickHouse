#include <Storages/RabbitMQ/RabbitMQBlockInputStream.h>

#include <DataStreams/ConvertingBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>

#include <Formats/FormatFactory.h>
#include <Processors/Formats/InputStreamFromInputFormat.h>

#include <Storages/RabbitMQ/ReadBufferFromRabbitMQConsumer.h>

namespace DB
{

RabbitMQBlockInputStream::RabbitMQBlockInputStream(
        StorageRabbitMQ & storage_, const Context & context_, const Names & columns)
        : storage(storage_)
        , context(context_)
        , column_names(columns)
{
}

/// These functions are to be implemented

RabbitMQBlockInputStream::~RabbitMQBlockInputStream()
{
    storage.pushReadBuffer(buffer);
}

Block RabbitMQBlockInputStream::getHeader() const
{
    return storage.getSampleBlockForColumns(column_names);
}


void RabbitMQBlockInputStream::readPrefixImpl()
{
}

Block RabbitMQBlockInputStream::readImpl() { return Block(); }

void RabbitMQBlockInputStream::readSuffixImpl()
{
}

void RabbitMQBlockInputStream::commit()
{
    if (!buffer)
        return;

    buffer->commit();
}
}