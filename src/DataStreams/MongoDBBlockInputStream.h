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

void authenticate(Poco::MongoDB::Connection & connection, const std::string & database, const std::string & user, const std::string & password);

std::unique_ptr<Poco::MongoDB::Cursor> createCursor(const std::string & database, const std::string & collection, const Block & sample_block_to_select);

/// Converts MongoDB Cursor to a stream of Blocks
class MongoDBBlockInputStream final : public IBlockInputStream
{
public:
    MongoDBBlockInputStream(
        std::shared_ptr<Poco::MongoDB::Connection> & connection_,
        std::unique_ptr<Poco::MongoDB::Cursor> cursor_,
        const Block & sample_block,
        UInt64 max_block_size_,
        bool strict_check_names_ = false);

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

    /// if true stream will check, that all required fields present in MongoDB
    /// collection, otherwise throw exception.
    bool strict_check_names;
};

}
