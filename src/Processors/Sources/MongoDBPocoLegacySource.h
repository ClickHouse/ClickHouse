#pragma once

#include "config.h"

#if USE_MONGODB
#include <Poco/MongoDB/Element.h>
#include <Poco/MongoDB/Array.h>

#include <Core/Block.h>
#include <Processors/ISource.h>
#include <Core/ExternalResultDescription.h>

#include <Core/Field.h>


namespace Poco
{
namespace MongoDB
{
    class Connection;
    class Document;
    class Cursor;
    class OpMsgCursor;
}
}

namespace DB
{

struct MongoDBPocoLegacyArrayInfo
{
    size_t num_dimensions;
    Field default_value;
    std::function<Field(const Poco::MongoDB::Element & value, const std::string & name)> parser;
};

void authenticate(Poco::MongoDB::Connection & connection, const std::string & database, const std::string & user, const std::string & password);

bool isMongoDBWireProtocolOld(Poco::MongoDB::Connection & connection_, const std::string & database_name_);

/// Deprecated, will be removed soon.
class MongoDBPocoLegacyCursor
{
public:
    MongoDBPocoLegacyCursor(
        const std::string & database,
        const std::string & collection,
        const Block & sample_block_to_select,
        const Poco::MongoDB::Document & query,
        Poco::MongoDB::Connection & connection);

    Poco::MongoDB::Document::Vector nextDocuments(Poco::MongoDB::Connection & connection);

    Int64 cursorID() const;

private:
    const bool is_wire_protocol_old;
    std::unique_ptr<Poco::MongoDB::Cursor> old_cursor;
    std::unique_ptr<Poco::MongoDB::OpMsgCursor> new_cursor;
    Int64 cursor_id = 0;
};

/// Converts MongoDB Cursor to a stream of Blocks. Deprecated, will be removed soon.
class MongoDBPocoLegacySource final : public ISource
{
public:
    MongoDBPocoLegacySource(
        std::shared_ptr<Poco::MongoDB::Connection> & connection_,
        const String & database_name_,
        const String & collection_name_,
        const Poco::MongoDB::Document & query_,
        const Block & sample_block,
        UInt64 max_block_size_);

    ~MongoDBPocoLegacySource() override;

    String getName() const override { return "MongoDB"; }

private:
    Chunk generate() override;

    std::shared_ptr<Poco::MongoDB::Connection> connection;
    MongoDBPocoLegacyCursor cursor;
    const UInt64 max_block_size;
    ExternalResultDescription description;
    bool all_read = false;

    std::unordered_map<size_t, MongoDBPocoLegacyArrayInfo> array_info;
};

}
#endif
