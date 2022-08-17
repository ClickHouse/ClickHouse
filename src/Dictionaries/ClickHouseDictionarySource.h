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
        const std::string db;
        const std::string table;
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
        ContextPtr context);

    /// copy-constructor is provided in order to support cloneability
    ClickHouseDictionarySource(const ClickHouseDictionarySource & other);
    ClickHouseDictionarySource & operator=(const ClickHouseDictionarySource &) = delete;

    BlockInputStreamPtr loadAllWithSizeHint(std::atomic<size_t> * result_size_hint) override;

    BlockInputStreamPtr loadAll() override;

    BlockInputStreamPtr loadUpdatedAll() override;

    BlockInputStreamPtr loadIds(const std::vector<UInt64> & ids) override;

    BlockInputStreamPtr loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows) override;

    bool isModified() const override;
    bool supportsSelectiveLoad() const override { return true; }

    bool hasUpdateField() const override;

    DictionarySourcePtr clone() const override { return std::make_unique<ClickHouseDictionarySource>(*this); }

    std::string toString() const override;

    /// Used for detection whether the hashtable should be preallocated
    /// (since if there is WHERE then it can filter out too much)
    bool hasWhere() const { return !configuration.where.empty(); }

private:
    std::string getUpdateFieldAndDate();

    BlockInputStreamPtr createStreamForQuery(const String & query, std::atomic<size_t> * result_size_hint = nullptr);

    std::string doInvalidateQuery(const std::string & request) const;

    std::chrono::time_point<std::chrono::system_clock> update_time;
    const DictionaryStructure dict_struct;
    const Configuration configuration;
    mutable std::string invalidate_query_response;
    ExternalQueryBuilder query_builder;
    Block sample_block;
    ContextMutablePtr context;
    ConnectionPoolWithFailoverPtr pool;
    const std::string load_all_query;
    Poco::Logger * log = &Poco::Logger::get("ClickHouseDictionarySource");
};

}
