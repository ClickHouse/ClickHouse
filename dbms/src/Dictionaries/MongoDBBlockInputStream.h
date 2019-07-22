#pragma once

#include <Core/Block.h>
#include <DataStreams/IBlockInputStream.h>
#include <Core/ExternalResultDescription.h>


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
class MongoDBBlockInputStream final : public IBlockInputStream
{
public:
    MongoDBBlockInputStream(
        std::shared_ptr<Poco::MongoDB::Connection> & connection_,
        std::unique_ptr<Poco::MongoDB::Cursor> cursor_,
        const Block & sample_block,
        const UInt64 max_block_size);

    ~MongoDBBlockInputStream() override;

    String getName() const override { return "MongoDB"; }

    Block getHeader() const override { return description.sample_block.cloneEmpty(); }

private:
    Block readImpl() override;

    std::shared_ptr<Poco::MongoDB::Connection> connection;
    std::unique_ptr<Poco::MongoDB::Cursor> cursor;
    const UInt64 max_block_size;
    ExternalResultDescription description;
    bool all_read = false;
};

}
