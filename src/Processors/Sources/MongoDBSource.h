#pragma once

#include <Poco/MongoDB/Element.h>

#include <Core/Block.h>
#include <Processors/ISource.h>
#include <Core/ExternalResultDescription.h>

#include <Core/Field.h>


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

struct MongoDBArrayInfo
{
    size_t num_dimensions;
    Field default_value;
    std::function<Field(const Poco::MongoDB::Element & value, const std::string & name)> parser;
};

void authenticate(Poco::MongoDB::Connection & connection, const std::string & database, const std::string & user, const std::string & password);

std::unique_ptr<Poco::MongoDB::Cursor> createCursor(const std::string & database, const std::string & collection, const Block & sample_block_to_select);

/// Converts MongoDB Cursor to a stream of Blocks
class MongoDBSource final : public ISource
{
public:
    MongoDBSource(
        std::shared_ptr<Poco::MongoDB::Connection> & connection_,
        std::unique_ptr<Poco::MongoDB::Cursor> cursor_,
        const Block & sample_block,
        UInt64 max_block_size_);

    ~MongoDBSource() override;

    String getName() const override { return "MongoDB"; }

private:
    Chunk generate() override;

    std::shared_ptr<Poco::MongoDB::Connection> connection;
    std::unique_ptr<Poco::MongoDB::Cursor> cursor;
    const UInt64 max_block_size;
    ExternalResultDescription description;
    bool all_read = false;

    std::unordered_map<size_t, MongoDBArrayInfo> array_info;
};

}
