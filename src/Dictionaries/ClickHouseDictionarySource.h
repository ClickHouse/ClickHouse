#pragma once

#include <memory>
#include <Poco/Logger.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <Interpreters/Context.h>
#include "DictionaryStructure.h"
#include "ExternalQueryBuilder.h"
#include "IDictionarySource.h"


namespace DB
{
/** Allows loading dictionaries from local or remote ClickHouse instance
  *    @todo use ConnectionPoolWithFailover
  *    @todo invent a way to keep track of source modifications
  */
class ClickHouseDictionarySource final : public IDictionarySource
{
public:
    struct Configuration
    {
        const std::string host;
        const std::string user;
        const std::string password;
        const std::string proto_send_chunked;
        const std::string proto_recv_chunked;
        const std::string quota_key;
        const std::string db;
        const std::string table;
        const std::string query;
        const std::string where;
        const std::string invalidate_query;
        const std::string update_field;
        const UInt64 update_lag;
        const UInt16 port;
        const bool is_local;
        const bool secure;
    };

    ClickHouseDictionarySource(
        const DictionaryStructure & dict_struct_,
        const Configuration & configuration_,
        const Block & sample_block_,
        ContextMutablePtr context_);

    /// copy-constructor is provided in order to support cloneability
    ClickHouseDictionarySource(const ClickHouseDictionarySource & other);
    ClickHouseDictionarySource & operator=(const ClickHouseDictionarySource &) = delete;

    QueryPipeline loadAll() override;

    QueryPipeline loadUpdatedAll() override;

    QueryPipeline loadIds(const std::vector<UInt64> & ids) override;

    QueryPipeline loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows) override;

    bool isModified() const override;
    bool supportsSelectiveLoad() const override { return true; }

    bool hasUpdateField() const override;

    bool isLocal() const { return configuration.is_local; }

    DictionarySourcePtr clone() const override { return std::make_shared<ClickHouseDictionarySource>(*this); }

    std::string toString() const override;

    /// Used for detection whether the hashtable should be preallocated
    /// (since if there is WHERE then it can filter out too much)
    bool hasWhere() const { return !configuration.where.empty(); }

private:
    std::string getUpdateFieldAndDate();

    QueryPipeline createStreamForQuery(const String & query);

    std::string doInvalidateQuery(const std::string & request) const;

    std::chrono::time_point<std::chrono::system_clock> update_time;
    const DictionaryStructure dict_struct;
    const Configuration configuration;
    mutable std::string invalidate_query_response;
    ExternalQueryBuilderPtr query_builder;
    Block sample_block;
    ContextMutablePtr context;
    ConnectionPoolWithFailoverPtr pool;
    std::string load_all_query;
    LoggerPtr log = getLogger("ClickHouseDictionarySource");

    /// RegExpTreeDictionary is the only dictionary whose structure of attributions differ from the input block.
    /// For now we need to modify sample_block in the ctor of RegExpTreeDictionary.
    friend class RegExpTreeDictionary;
};

}
