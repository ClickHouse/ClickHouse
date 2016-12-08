#pragma once

#include <DB/Dictionaries/IDictionarySource.h>
#include <DB/Dictionaries/ExternalQueryBuilder.h>
#include <DB/Dictionaries/DictionaryStructure.h>


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
		const ConstColumnPlainPtrs & key_columns, const std::vector<std::size_t> & requested_rows) override;

	bool isModified() const override;

	bool supportsSelectiveLoad() const override;

	DictionarySourcePtr clone() const override;

	std::string toString() const override;

private:
	Logger * log = &Logger::get("ODBCDictionarySource");

	const DictionaryStructure dict_struct;
	const std::string db;
	const std::string table;
	const std::string where;
	Block sample_block;
	std::shared_ptr<Poco::Data::SessionPool> pool;
	ExternalQueryBuilder query_builder;
	const std::string load_all_query;
};


}
