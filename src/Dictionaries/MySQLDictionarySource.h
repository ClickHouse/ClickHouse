#pragma once

#include <Core/Block.h>

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL
#    include <common/LocalDateTime.h>
#    include <mysqlxx/PoolWithFailover.h>
#    include "DictionaryStructure.h"
#    include "ExternalQueryBuilder.h"
#    include "IDictionarySource.h"
#    include <Formats/MySQLBlockInputStream.h>

namespace Poco
{
class Logger;

namespace Util
{
    class AbstractConfiguration;
}
}


namespace DB
{
/// Allows loading dictionaries from a MySQL database
class MySQLDictionarySource final : public IDictionarySource
{
public:
    struct Configuration
    {
        const std::string db;
        const std::string table;
        const std::string where;
        const std::string invalidate_query;
        const std::string update_field;
        const UInt64 update_lag;
        const bool dont_check_update_time;
    };

    MySQLDictionarySource(
        const DictionaryStructure & dict_struct_,
        const Configuration & configuration_,
        mysqlxx::PoolWithFailoverPtr pool_,
        const Block & sample_block_,
        const StreamSettings & settings_);

    /// copy-constructor is provided in order to support cloneability
    MySQLDictionarySource(const MySQLDictionarySource & other);
    MySQLDictionarySource & operator=(const MySQLDictionarySource &) = delete;

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
    BlockInputStreamPtr loadFromQuery(const String & query);

    std::string getUpdateFieldAndDate();

    static std::string quoteForLike(const std::string & value);

    LocalDateTime getLastModification(mysqlxx::Pool::Entry & connection, bool allow_connection_closure) const;

    // execute invalidate_query. expects single cell in result
    std::string doInvalidateQuery(const std::string & request) const;

    Poco::Logger * log;

    std::chrono::time_point<std::chrono::system_clock> update_time;
    const DictionaryStructure dict_struct;
    const Configuration configuration;
    mysqlxx::PoolWithFailoverPtr pool;
    Block sample_block;
    ExternalQueryBuilder query_builder;
    const std::string load_all_query;
    LocalDateTime last_modification;
    mutable std::string invalidate_query_response;
    const StreamSettings settings;
};

}

#endif
