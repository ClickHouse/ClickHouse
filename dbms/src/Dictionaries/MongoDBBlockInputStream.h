#pragma once

#include <Core/Block.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Dictionaries/ExternalResultDescription.h>


namespace Poco
{
    namespace MongoDB
    {
        class Connection;
        class Cursor;
    }
}


namespace DB
{

/// Converts MongoDB Cursor to a stream of Blocks
class MongoDBBlockInputStream final : public IProfilingBlockInputStream
{
public:
    MongoDBBlockInputStream(
        std::shared_ptr<Poco::MongoDB::Connection> & connection_,
        std::unique_ptr<Poco::MongoDB::Cursor> cursor_,
        const Block & sample_block,
        const size_t max_block_size);

    ~MongoDBBlockInputStream() override;

    String getName() const override { return "MongoDB"; }

    String getID() const override;

private:
    Block readImpl() override;

    static void insertDefaultValue(IColumn * column, const IColumn & sample_column)
    {
        column->insertFrom(sample_column, 0);
    }

    std::shared_ptr<Poco::MongoDB::Connection> connection;
    std::unique_ptr<Poco::MongoDB::Cursor> cursor;
    const size_t max_block_size;
    ExternalResultDescription description;
    bool all_read = false;
};

}
