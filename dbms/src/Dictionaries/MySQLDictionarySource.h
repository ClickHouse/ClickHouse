#pragma once

#include <Common/config.h>
#if USE_MYSQL

#    include <common/LocalDateTime.h>
#    include <mysqlxx/PoolWithFailover.h>
#    include "DictionaryStructure.h"
#    include "ExternalQueryBuilder.h"
#    include "IDictionarySource.h"


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
    MySQLDictionarySource(
        const DictionaryStructure & dict_struct_,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const Block & sample_block);

    /// copy-constructor is provided in order to support cloneability
    MySQLDictionarySource(const MySQLDictionarySource & other);

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

    static std::string quoteForLike(const std::string s);

    LocalDateTime getLastModification() const;

    // execute invalidate_query. expects single cell in result
    std::string doInvalidateQuery(const std::string & request) const;

    Poco::Logger * log;

    std::chrono::time_point<std::chrono::system_clock> update_time;
    const DictionaryStructure dict_struct;
    const std::string db;
    const std::string table;
    const std::string where;
    const std::string update_field;
    const bool dont_check_update_time;
    Block sample_block;
    mutable mysqlxx::PoolWithFailover pool;
    ExternalQueryBuilder query_builder;
    const std::string load_all_query;
    LocalDateTime last_modification;
    std::string invalidate_query;
    mutable std::string invalidate_query_response;
};

}

#endif
