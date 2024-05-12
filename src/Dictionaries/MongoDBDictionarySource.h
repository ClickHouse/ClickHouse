#pragma once

#include "config.h"

#if USE_MONGODB
#include <Processors/Sources/MongoDBSource.h>
#include <Core/Block.h>

#include "DictionaryStructure.h"
#include "IDictionarySource.h"

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
        const std::string & uri_str_,
        const std::string & host_,
        const UInt16 & port_,
        const std::string & username_,
        const std::string & password_,
        const std::string & database_name_,
        const std::string & collection_,
        const std::string & options_,
        Block & sample_block_);

    MongoDBDictionarySource(
        const DictionaryStructure & dict_struct_,
        const mongocxx::uri & uri_,
        const std::string & collection_,
        Block & sample_block_);

    MongoDBDictionarySource(const MongoDBDictionarySource & other);

    ~MongoDBDictionarySource() override;

    QueryPipeline loadAll() override;

    QueryPipeline loadUpdatedAll() override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method loadUpdatedAll is unsupported for MongoDBDictionarySource");
    }

    bool supportsSelectiveLoad() const override { return true; }

    QueryPipeline loadIds(const std::vector<UInt64> & ids) override;

    QueryPipeline loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows) override;

    /// @todo: for MongoDB, modification date can somehow be determined from the `_id` object field
    bool isModified() const override { return true; }

    /// Not yet supported
    bool hasUpdateField() const override { return false; }

    DictionarySourcePtr clone() const override { return std::make_shared<MongoDBDictionarySource>(*this); }

    std::string toString() const override;

private:
    const DictionaryStructure dict_struct;

    mongocxx::uri uri;
    std::string collection;

    Block & sample_block;
};

}
#endif
