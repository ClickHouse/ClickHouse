#include <DB/Dictionaries/MySQLDictionarySource.h>


namespace DB
{


static const size_t max_block_size = 8192;


MySQLDictionarySource::MySQLDictionarySource(const DictionaryStructure & dict_struct_,
	const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix,
	const Block & sample_block)
	: dict_struct{dict_struct_},
	  db{config.getString(config_prefix + ".db", "")},
	  table{config.getString(config_prefix + ".table")},
	  where{config.getString(config_prefix + ".where", "")},
	  dont_check_update_time{config.getBool(config_prefix + ".dont_check_update_time", false)},
	  sample_block{sample_block},
	  pool{config, config_prefix},
	  query_builder{dict_struct, db, table, where},
	  load_all_query{query_builder.composeLoadAllQuery()}
{
}

/// copy-constructor is provided in order to support cloneability
MySQLDictionarySource::MySQLDictionarySource(const MySQLDictionarySource & other)
	: dict_struct{other.dict_struct},
	  db{other.db},
	  table{other.table},
	  where{other.where},
	  dont_check_update_time{other.dont_check_update_time},
	  sample_block{other.sample_block},
	  pool{other.pool},
	  query_builder{dict_struct, db, table, where},
	  load_all_query{other.load_all_query}, last_modification{other.last_modification}
{
}

BlockInputStreamPtr MySQLDictionarySource::loadAll()
{
	last_modification = getLastModification();

	LOG_TRACE(log, load_all_query);
	return std::make_shared<MySQLBlockInputStream>(pool.Get(), load_all_query, sample_block, max_block_size);
}

BlockInputStreamPtr MySQLDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
	/// Здесь не логгируем и не обновляем время модификации, так как запрос может быть большим, и часто задаваться.

	const auto query = query_builder.composeLoadIdsQuery(ids);
	return std::make_shared<MySQLBlockInputStream>(pool.Get(), query, sample_block, max_block_size);
}

BlockInputStreamPtr MySQLDictionarySource::loadKeys(
	const ConstColumnPlainPtrs & key_columns, const std::vector<std::size_t> & requested_rows)
{
	/// Здесь не логгируем и не обновляем время модификации, так как запрос может быть большим, и часто задаваться.

	const auto query = query_builder.composeLoadKeysQuery(key_columns, requested_rows, ExternalQueryBuilder::AND_OR_CHAIN);
	return std::make_shared<MySQLBlockInputStream>(pool.Get(), query, sample_block, max_block_size);
}

bool MySQLDictionarySource::isModified() const
{
	if (dont_check_update_time)
		return true;

	return getLastModification() > last_modification;
}

bool MySQLDictionarySource::supportsSelectiveLoad() const
{
	return true;
}

DictionarySourcePtr MySQLDictionarySource::clone() const
{
	return std::make_unique<MySQLDictionarySource>(*this);
}

std::string MySQLDictionarySource::toString() const
{
	return "MySQL: " + db + '.' + table + (where.empty() ? "" : ", where: " + where);
}

std::string MySQLDictionarySource::quoteForLike(const std::string s)
{
	std::string tmp;
	tmp.reserve(s.size());

	for (auto c : s)
	{
		if (c == '%' || c == '_' || c == '\\')
			tmp.push_back('\\');
		tmp.push_back(c);
	}

	std::string res;
	{
		WriteBufferFromString out(res);
		writeQuoted(tmp, out);
	}
	return res;
}

LocalDateTime MySQLDictionarySource::getLastModification() const
{
	LocalDateTime update_time{std::time(nullptr)};

	if (dont_check_update_time)
		return update_time;

	try
	{
		auto connection = pool.Get();
		auto query = connection->query("SHOW TABLE STATUS LIKE " + quoteForLike(table));

		LOG_TRACE(log, query.str());

		auto result = query.use();

		size_t fetched_rows = 0;
		if (auto row = result.fetch())
		{
			++fetched_rows;
			const auto UPDATE_TIME_IDX = 12;
			const auto & update_time_value = row[UPDATE_TIME_IDX];

			if (!update_time_value.isNull())
			{
				update_time = update_time_value.getDateTime();
				LOG_TRACE(log, "Got update time: " << update_time);
			}

			/// fetch remaining rows to avoid "commands out of sync" error
			while (result.fetch())
				++fetched_rows;
		}

		if (0 == fetched_rows)
			LOG_ERROR(log, "Cannot find table in SHOW TABLE STATUS result.");

		if (fetched_rows > 1)
			LOG_ERROR(log, "Found more than one table in SHOW TABLE STATUS result.");
	}
	catch (...)
	{
		tryLogCurrentException("MySQLDictionarySource");
	}

	/// we suppose failure to get modification time is not an error, therefore return current time
	return update_time;
}


}
