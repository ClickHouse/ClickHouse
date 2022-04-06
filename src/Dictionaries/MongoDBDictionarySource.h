#pragma once

#include <Core/Block.h>

#include "DictionaryStructure.h"
#include "IDictionarySource.h"

namespace Poco
{
namespace Util
{
    class AbstractConfiguration;
}

namespace MongoDB
{
    class Connection;
    class Cursor;
}
}

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

/// Allows loading dictionaries from a MongoDB collection
class MongoDBDictionarySource final : public IDictionarySource
{
public:
    MongoDBDictionarySource(
        const DictionaryStructure & dict_struct_,
        const std::string & uri_,
        const std::string & host_,
        UInt16 port_,
        const std::string & user_,
        const std::string & password_,
        const std::string & method_,
        const std::string & db_,
        const std::string & collection_,
        const Block & sample_block_);

    MongoDBDictionarySource(const MongoDBDictionarySource & other);

    ~MongoDBDictionarySource() override;

    Pipe loadAll() override;

    Pipe loadUpdatedAll() override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method loadUpdatedAll is unsupported for MongoDBDictionarySource");
    }

    bool supportsSelectiveLoad() const override { return true; }

    Pipe loadIds(const std::vector<UInt64> & ids) override;

    Pipe loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows) override;

    /// @todo: for MongoDB, modification date can somehow be determined from the `_id` object field
    bool isModified() const override { return true; }

    ///Not yet supported
    bool hasUpdateField() const override { return false; }

    DictionarySourcePtr clone() const override { return std::make_shared<MongoDBDictionarySource>(*this); }

    std::string toString() const override;

private:
    const DictionaryStructure dict_struct;
    const std::string uri;
    std::string host;
    UInt16 port;
    std::string user;
    const std::string password;
    const std::string method;
    std::string db;
    const std::string collection;
    Block sample_block;

    std::shared_ptr<Poco::MongoDB::Connection> connection;
};

}
