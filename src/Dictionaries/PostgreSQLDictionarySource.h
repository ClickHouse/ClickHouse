#pragma once

#include "config.h"
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/IDictionarySource.h>

#if USE_LIBPQXX
#include <Dictionaries/ExternalQueryBuilder.h>
#include <Core/Block.h>
#include <Common/LocalDateTime.h>
#include <Core/PostgreSQL/PoolWithFailover.h>


namespace DB
{

/// Allows loading dictionaries from a PostgreSQL database
class PostgreSQLDictionarySource final : public IDictionarySource
{
public:
    struct Configuration
    {
        const String db;
        const String schema;
        const String table;
        const String query;
        const String where;
        const String invalidate_query;
        const String update_field;
        const UInt64 update_lag;
    };

    PostgreSQLDictionarySource(
        const DictionaryStructure & dict_struct_,
        const Configuration & configuration_,
        postgres::PoolWithFailoverPtr pool_,
        const Block & sample_block_);

    /// copy-constructor is provided in order to support cloneability
    PostgreSQLDictionarySource(const PostgreSQLDictionarySource & other);
    PostgreSQLDictionarySource & operator=(const PostgreSQLDictionarySource &) = delete;

    QueryPipeline loadAll() override;
    QueryPipeline loadUpdatedAll() override;
    QueryPipeline loadIds(const std::vector<UInt64> & ids) override;
    QueryPipeline loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows) override;

    bool isModified() const override;
    bool supportsSelectiveLoad() const override;
    bool hasUpdateField() const override;

    DictionarySourcePtr clone() const override;
    std::string toString() const override;

private:
    String getUpdateFieldAndDate();
    String doInvalidateQuery(const std::string & request) const;
    QueryPipeline loadBase(const String & query);

    const DictionaryStructure dict_struct;
    const Configuration configuration;
    postgres::PoolWithFailoverPtr pool;
    Block sample_block;
    LoggerPtr log;
    ExternalQueryBuilder query_builder;
    const std::string load_all_query;
    std::chrono::time_point<std::chrono::system_clock> update_time;
    mutable std::string invalidate_query_response;

};

}
#endif
