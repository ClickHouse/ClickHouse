#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif
#include "DictionaryStructure.h"
#include "IDictionarySource.h"

#if USE_LIBPQXX
#include "ExternalQueryBuilder.h"
#include <Core/Block.h>
#include <common/LocalDateTime.h>
#include <common/logger_useful.h>
#include <Storages/PostgreSQL/PostgreSQLReplicaConnection.h>
#include <pqxx/pqxx>


namespace DB
{

/// Allows loading dictionaries from a PostgreSQL database
class PostgreSQLDictionarySource final : public IDictionarySource
{
public:
    PostgreSQLDictionarySource(
        const DictionaryStructure & dict_struct_,
        const Poco::Util::AbstractConfiguration & config_,
        const std::string & config_prefix,
        const Block & sample_block_);

    /// copy-constructor is provided in order to support cloneability
    PostgreSQLDictionarySource(const PostgreSQLDictionarySource & other);
    PostgreSQLDictionarySource & operator=(const PostgreSQLDictionarySource &) = delete;

    BlockInputStreamPtr loadAll() override;
    BlockInputStreamPtr loadUpdatedAll() override;
    BlockInputStreamPtr loadIds(const std::vector<UInt64> & ids) override;
    BlockInputStreamPtr loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows) override;

    bool isModified() const override;
    bool supportsSelectiveLoad() const override;
    bool hasUpdateField() const override;

    DictionarySourcePtr clone() const override;
    std::string toString() const override;

private:
    std::string getUpdateFieldAndDate();
    std::string doInvalidateQuery(const std::string & request) const;
    BlockInputStreamPtr loadBase(const String & query);

    const DictionaryStructure dict_struct;
    Block sample_block;
    PostgreSQLReplicaConnectionPtr connection;
    Poco::Logger * log;

    const std::string db;
    const std::string table;
    const std::string where;
    ExternalQueryBuilder query_builder;
    const std::string load_all_query;
    std::string invalidate_query;
    std::chrono::time_point<std::chrono::system_clock> update_time;
    const std::string update_field;
    mutable std::string invalidate_query_response;

};

}
#endif
