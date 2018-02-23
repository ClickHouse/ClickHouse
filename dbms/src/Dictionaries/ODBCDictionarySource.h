#pragma once

#include <Dictionaries/IDictionarySource.h>
#include <Dictionaries/ExternalQueryBuilder.h>
#include <Dictionaries/DictionaryStructure.h>


namespace Poco
{
    namespace Data
    {
        class SessionPool;
    }

    namespace Util
    {
        class AbstractConfiguration;
    }

    class Logger;
}


namespace DB
{


/// Allows loading dictionaries from a ODBC source
class ODBCDictionarySource final : public IDictionarySource
{
public:
    ODBCDictionarySource(const DictionaryStructure & dict_struct_,
        const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix,
        const Block & sample_block);

    /// copy-constructor is provided in order to support cloneability
    ODBCDictionarySource(const ODBCDictionarySource & other);

    BlockInputStreamPtr loadAll() override;

    BlockInputStreamPtr loadIds(const std::vector<UInt64> & ids) override;

    BlockInputStreamPtr loadKeys(
        const Columns & key_columns, const std::vector<size_t> & requested_rows) override;

    bool isModified() const override;

    bool supportsSelectiveLoad() const override;

    DictionarySourcePtr clone() const override;

    std::string toString() const override;

private:
    // execute invalidate_query. expects single cell in result
    std::string doInvalidateQuery(const std::string & request) const;

    Poco::Logger * log;

    const DictionaryStructure dict_struct;
    const std::string db;
    const std::string table;
    const std::string where;
    Block sample_block;
    std::shared_ptr<Poco::Data::SessionPool> pool = nullptr;
    ExternalQueryBuilder query_builder;
    const std::string load_all_query;
    std::string invalidate_query;
    mutable std::string invalidate_query_response;
};


}
