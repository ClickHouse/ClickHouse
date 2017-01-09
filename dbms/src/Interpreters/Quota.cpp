#include <iomanip>

#include <common/logger_useful.h>

#include <DB/Common/SipHash.h>
#include <DB/Common/StringUtils.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/Interpreters/Quota.h>

#include <set>


namespace DB
{

namespace ErrorCodes
{
	extern const int QUOTA_EXPIRED;
	extern const int QUOTA_DOESNT_ALLOW_KEYS;
	extern const int UNKNOWN_QUOTA;
}


template <typename Counter>
void QuotaValues<Counter>::initFromConfig(const String & config_elem, Poco::Util::AbstractConfiguration & config)
{
	queries 		= parse<UInt64>(config.getString(config_elem + ".queries", 		"0"));
	errors 			= parse<UInt64>(config.getString(config_elem + ".errors", 		"0"));
	result_rows 	= parse<UInt64>(config.getString(config_elem + ".result_rows",	"0"));
	result_bytes 	= parse<UInt64>(config.getString(config_elem + ".result_bytes",	"0"));
	read_rows 		= parse<UInt64>(config.getString(config_elem + ".read_rows", 	"0"));
	read_bytes 		= parse<UInt64>(config.getString(config_elem + ".read_bytes", 	"0"));
	execution_time_usec = config.getUInt64(config_elem + ".execution_time", 0) * 1000000ULL;
}

template void QuotaValues<size_t>::initFromConfig(const String & config_elem, Poco::Util::AbstractConfiguration & config);
template void QuotaValues<std::atomic<size_t>>::initFromConfig(const String & config_elem, Poco::Util::AbstractConfiguration & config);


void QuotaForInterval::initFromConfig(const String & config_elem, time_t duration_, time_t offset_, Poco::Util::AbstractConfiguration & config)
{
	rounded_time = 0;
	duration = duration_;
	offset = offset_;
	max.initFromConfig(config_elem, config);
}

void QuotaForInterval::checkExceeded(time_t current_time, const String & quota_name, const String & user_name)
{
	updateTime(current_time);
	check(max.queries, used.queries, current_time, quota_name, user_name, "Queries");
	check(max.errors, used.errors, current_time, quota_name, user_name, "Errors");
	check(max.result_rows, used.result_rows, current_time, quota_name, user_name, "Total result rows");
	check(max.result_bytes, used.result_bytes, current_time, quota_name, user_name, "Total result bytes");
	check(max.read_rows, used.read_rows, current_time, quota_name, user_name, "Total rows read");
	check(max.read_bytes, used.read_bytes, current_time, quota_name, user_name, "Total bytes read");
	check(max.execution_time_usec / 1000000, used.execution_time_usec / 1000000, current_time, quota_name, user_name, "Total execution time");
}

String QuotaForInterval::toString() const
{
	std::stringstream res;

	res << std::fixed << std::setprecision(3)
		<< "Interval:       " << LocalDateTime(rounded_time) << " - " << LocalDateTime(rounded_time + duration) << ".\n"
		<< "Queries:        " << used.queries 		<< ".\n"
		<< "Errors:         " << used.errors 		<< ".\n"
		<< "Result rows:    " << used.result_rows 	<< ".\n"
		<< "Result bytes:   " << used.result_bytes 	<< ".\n"
		<< "Read rows:      " << used.read_rows 	<< ".\n"
		<< "Read bytes:     " << used.read_bytes 	<< ".\n"
		<< "Execution time: " << used.execution_time_usec / 1000000.0 << " sec.\n";

	return res.str();
}

void QuotaForInterval::addQuery() noexcept
{
	++used.queries;
}

void QuotaForInterval::addError() noexcept
{
	++used.errors;
}

void QuotaForInterval::checkAndAddResultRowsBytes(time_t current_time, const String & quota_name, const String & user_name, size_t rows, size_t bytes)
{
	used.result_rows += rows;
	used.result_bytes += bytes;
	checkExceeded(current_time, quota_name, user_name);
}

void QuotaForInterval::checkAndAddReadRowsBytes(time_t current_time, const String & quota_name, const String & user_name, size_t rows, size_t bytes)
{
	used.read_rows += rows;
	used.read_bytes += bytes;
	checkExceeded(current_time, quota_name, user_name);
}

void QuotaForInterval::checkAndAddExecutionTime(time_t current_time, const String & quota_name, const String & user_name, Poco::Timespan amount)
{
	/// Используется информация о внутреннем представлении Poco::Timespan.
	used.execution_time_usec += amount.totalMicroseconds();
	checkExceeded(current_time, quota_name, user_name);
}

void QuotaForInterval::updateTime(time_t current_time)
{
	if (current_time >= rounded_time + static_cast<int>(duration))
	{
		rounded_time = (current_time - offset) / duration * duration + offset;
		used.clear();
	}
}

void QuotaForInterval::check(
	size_t max_amount, size_t used_amount, time_t current_time,
	const String & quota_name, const String & user_name, const char * resource_name)
{
	if (max_amount && used_amount > max_amount)
	{
		std::stringstream message;
		message << "Quota for user '" << user_name << "' for ";

		if (duration == 3600)
			message << "1 hour";
		else if (duration == 60)
			message << "1 minute";
		else if (duration % 3600 == 0)
			message << (duration / 3600) << " hours";
		else if (duration % 60 == 0)
			message << (duration / 60) << " minutes";
		else
			message << duration << " seconds";

		message << " has been exceeded. "
			<< resource_name << ": " << used_amount << ", max: " << max_amount << ". "
			<< "Interval will end at " << LocalDateTime(rounded_time + duration) << ". "
			<< "Name of quota template: '" << quota_name << "'.";

		throw Exception(message.str(), ErrorCodes::QUOTA_EXPIRED);
	}
}


void QuotaForIntervals::initFromConfig(const String & config_elem, Poco::Util::AbstractConfiguration & config, std::mt19937 & rng)
{
	Poco::Util::AbstractConfiguration::Keys config_keys;
	config.keys(config_elem, config_keys);

	for (Poco::Util::AbstractConfiguration::Keys::const_iterator it = config_keys.begin(); it != config_keys.end(); ++it)
	{
		if (!startsWith(*it, "interval"))
			continue;

		String interval_config_elem = config_elem + "." + *it;
		time_t duration = config.getInt(interval_config_elem + ".duration", 0);
		time_t offset = 0;

		if (!duration) /// Skip quaotas with zero duration
			continue;

		bool randomize = config.getBool(interval_config_elem + ".randomize", false);
		if (randomize)
			offset = std::uniform_int_distribution<decltype(duration)>(0, duration - 1)(rng);

		cont[duration].initFromConfig(interval_config_elem, duration, offset, config);
	}
}

void QuotaForIntervals::setMax(const QuotaForIntervals & quota)
{
	for (Container::iterator it = cont.begin(); it != cont.end();)
	{
		if (quota.cont.count(it->first))
			++it;
		else
			cont.erase(it++);
	}

	for (auto & x : quota.cont)
	{
		if (!cont.count(x.first))
			cont[x.first] = x.second;
		else
			cont[x.first].max = x.second.max;
	}
}

void QuotaForIntervals::checkExceeded(time_t current_time)
{
	for (Container::reverse_iterator it = cont.rbegin(); it != cont.rend(); ++it)
		it->second.checkExceeded(current_time, quota_name, user_name);
}

void QuotaForIntervals::addQuery() noexcept
{
	for (Container::reverse_iterator it = cont.rbegin(); it != cont.rend(); ++it)
		it->second.addQuery();
}

void QuotaForIntervals::addError() noexcept
{
	for (Container::reverse_iterator it = cont.rbegin(); it != cont.rend(); ++it)
		it->second.addError();
}

void QuotaForIntervals::checkAndAddResultRowsBytes(time_t current_time, size_t rows, size_t bytes)
{
	for (Container::reverse_iterator it = cont.rbegin(); it != cont.rend(); ++it)
		it->second.checkAndAddResultRowsBytes(current_time, quota_name, user_name, rows, bytes);
}

void QuotaForIntervals::checkAndAddReadRowsBytes(time_t current_time, size_t rows, size_t bytes)
{
	for (Container::reverse_iterator it = cont.rbegin(); it != cont.rend(); ++it)
		it->second.checkAndAddReadRowsBytes(current_time, quota_name, user_name, rows, bytes);
}

void QuotaForIntervals::checkAndAddExecutionTime(time_t current_time, Poco::Timespan amount)
{
	for (Container::reverse_iterator it = cont.rbegin(); it != cont.rend(); ++it)
		it->second.checkAndAddExecutionTime(current_time, quota_name, user_name, amount);
}

String QuotaForIntervals::toString() const
{
	std::stringstream res;

	for (Container::const_reverse_iterator it = cont.rbegin(); it != cont.rend(); ++it)
		res << std::endl << it->second.toString();

	return res.str();
}


void Quota::loadFromConfig(const String & config_elem, const String & name_, Poco::Util::AbstractConfiguration & config, std::mt19937 & rng)
{
	name = name_;

	bool new_keyed_by_ip = config.has(config_elem + ".keyed_by_ip");
	bool new_is_keyed = new_keyed_by_ip || config.has(config_elem + ".keyed");

	if (new_is_keyed != is_keyed || new_keyed_by_ip != keyed_by_ip)
	{
		keyed_by_ip = new_keyed_by_ip;
		is_keyed = new_is_keyed;
		/// Meaning of keys has been changed. Throw away accumulated values.
		quota_for_keys.clear();
	}

	QuotaForIntervals new_max(name, {});
	new_max.initFromConfig(config_elem, config, rng);
	if (!(new_max == max))
	{
		max = new_max;
		for (auto & quota : quota_for_keys)
			quota.second->setMax(max);
	}
}

QuotaForIntervalsPtr Quota::get(const String & quota_key, const String & user_name, const Poco::Net::IPAddress & ip)
{
	if (!quota_key.empty() && (!is_keyed || keyed_by_ip))
		throw Exception("Quota " + name + " (for user " + user_name + ") doesn't allow client supplied keys.",
			ErrorCodes::QUOTA_DOESNT_ALLOW_KEYS);

	/** Quota is calculated separately:
	  * - for each IP-address, if 'keyed_by_ip';
	  * - otherwise for each 'quota_key', if present;
	  * - otherwise for each 'user_name'.
	  */

	UInt64 quota_key_hashed = sipHash64(
		keyed_by_ip
			? ip.toString()
			: (!quota_key.empty()
				? quota_key
				: user_name));

	std::lock_guard<std::mutex> lock(mutex);

	Container::iterator it = quota_for_keys.find(quota_key_hashed);
	if (quota_for_keys.end() == it)
		it = quota_for_keys.emplace(quota_key_hashed, std::make_shared<QuotaForIntervals>(max, user_name)).first;

	return it->second;
}


void Quotas::loadFromConfig(Poco::Util::AbstractConfiguration & config)
{
	std::mt19937 rng;

	Poco::Util::AbstractConfiguration::Keys config_keys;
	config.keys("quotas", config_keys);

	/// Remove keys, that now absent in config.
	std::set<std::string> keys_set(config_keys.begin(), config_keys.end());
	for (Container::iterator it = cont.begin(); it != cont.end();)
	{
		if (keys_set.count(it->first))
			++it;
		else
			cont.erase(it++);
	}

	for (Poco::Util::AbstractConfiguration::Keys::const_iterator it = config_keys.begin(); it != config_keys.end(); ++it)
	{
		if (!cont[*it])
			cont[*it] = std::make_unique<Quota>();
		cont[*it]->loadFromConfig("quotas." + *it, *it, config, rng);
	}
}

QuotaForIntervalsPtr Quotas::get(const String & name, const String & quota_key, const String & user_name, const Poco::Net::IPAddress & ip)
{
	Container::iterator it = cont.find(name);
	if (cont.end() == it)
		throw Exception("Unknown quota " + name, ErrorCodes::UNKNOWN_QUOTA);

	return it->second->get(quota_key, user_name, ip);
}

}
