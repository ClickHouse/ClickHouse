#pragma once

#include <Dictionaries/IDictionarySource.h>
#include <Dictionaries/DictionaryStructure.h>


namespace Poco
{
    namespace Util
    {
        class AbstractConfiguration;
    }

    namespace MongoDB
    {
        class Connection;
    }
}


namespace DB
{

/// Allows loading dictionaries from a MongoDB collection
class MongoDBDictionarySource final : public IDictionarySource
{
    MongoDBDictionarySource(
        const DictionaryStructure & dict_struct, const std::string & host, UInt16 port,
        const std::string & user, const std::string & password,
        const std::string & method,
        const std::string & db, const std::string & collection,
        const Block & sample_block);

public:
    MongoDBDictionarySource(
        const DictionaryStructure & dict_struct, const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix, Block & sample_block);

    MongoDBDictionarySource(const MongoDBDictionarySource & other);

    ~MongoDBDictionarySource() override;

    BlockInputStreamPtr loadAll() override;

    BlockInputStreamPtr loadUpdatedAll() override
    {
        throw Exception{"Method loadUpdatedAll is unsupported for MongoDBDictionarySource", ErrorCodes::NOT_IMPLEMENTED};
    }

    bool supportsSelectiveLoad() const override { return true; }

    BlockInputStreamPtr loadIds(const std::vector<UInt64> & ids) override;

    BlockInputStreamPtr loadKeys(
        const Columns & key_columns, const std::vector<size_t> & requested_rows) override;

    /// @todo: for MongoDB, modification date can somehow be determined from the `_id` object field
    bool isModified() const override { return true; }

    ///Not yet supported
    bool hasUpdateField() const override { return false; }

    DictionarySourcePtr clone() const override { return std::make_unique<MongoDBDictionarySource>(*this); }

    std::string toString() const override;

private:
    const DictionaryStructure dict_struct;
    const std::string host;
    const UInt16 port;
    const std::string user;
    const std::string password;
    const std::string method;
    const std::string db;
    const std::string collection;
    Block sample_block;

    std::shared_ptr<Poco::MongoDB::Connection> connection;
};

}
