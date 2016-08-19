#pragma once

#include <cstring>
#include <unordered_map>
#include <memory>
#include <random>

#include <Poco/Timespan.h>

#include <Poco/Util/Application.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <Poco/Net/IPAddress.h>

#include <common/Common.h>

#include <DB/Core/Types.h>
#include <DB/Common/Exception.h>
#include <DB/IO/WriteHelpers.h>


namespace DB
{

/** Квота на потребление ресурсов за заданный интервал - часть настроек.
  * Используются, чтобы ограничить потребление ресурсов пользователем.
  * Квота действует "мягко" - может быть немного превышена, так как проверяется, как правило, на каждый блок.
  * Квота не сохраняется при перезапуске сервера.
  * Квота распространяется только на один сервер.
  */

/// Used both for maximum allowed values and for counters of current accumulated values.
template <typename Counter>		/// either size_t or std::atomic<size_t>
struct QuotaValues
{
	/// Zero values (for maximums) means no limit.
	Counter queries;				/// Количество запросов.
	Counter errors;					/// Количество запросов с эксепшенами.
	Counter result_rows;			/// Количество строк, отданных в качестве результата.
	Counter result_bytes;			/// Количество байт, отданных в качестве результата.
	Counter read_rows;				/// Количество строк, прочитанных из таблиц.
	Counter read_bytes;				/// Количество байт, прочитанных из таблиц.
	Counter execution_time_usec;	/// Суммарное время выполнения запросов в микросекундах.

	QuotaValues()
	{
		clear();
	}

	QuotaValues(const QuotaValues & rhs)
	{
		tuple() = rhs.tuple();
	}

	QuotaValues & operator=(const QuotaValues & rhs)
	{
		tuple() = rhs.tuple();
		return *this;
	}

	void clear()
	{
		tuple() = std::make_tuple(0, 0, 0, 0, 0, 0, 0);
	}

	void initFromConfig(const String & config_elem, Poco::Util::AbstractConfiguration & config);

	bool operator== (const QuotaValues & rhs) const
	{
		return tuple() == rhs.tuple();
	}

private:
	auto tuple()
	{
		return std::forward_as_tuple(queries, errors, result_rows, result_bytes, read_rows, read_bytes, execution_time_usec);
	}

	auto tuple() const
	{
		return std::make_tuple(queries, errors, result_rows, result_bytes, read_rows, read_bytes, execution_time_usec);
	}
};

template <>
inline auto QuotaValues<std::atomic<size_t>>::tuple() const
{
	return std::make_tuple(
		queries.load(std::memory_order_relaxed),
		errors.load(std::memory_order_relaxed),
		result_rows.load(std::memory_order_relaxed),
		result_bytes.load(std::memory_order_relaxed),
		read_rows.load(std::memory_order_relaxed),
		read_bytes.load(std::memory_order_relaxed),
		execution_time_usec.load(std::memory_order_relaxed));
}


/// Время, округлённое до границы интервала, квота за интервал и накопившиеся за этот интервал значения.
struct QuotaForInterval
{
	time_t rounded_time = 0;
	size_t duration = 0;
	time_t offset = 0;		/// Offset of interval for randomization (to avoid DoS if intervals for many users end at one time).
	QuotaValues<size_t> max;
	QuotaValues<std::atomic<size_t>> used;

	QuotaForInterval() {}
	QuotaForInterval(time_t duration_) : duration(duration_) {}

	void initFromConfig(const String & config_elem, time_t duration_, time_t offset_, Poco::Util::AbstractConfiguration & config);

	/// Увеличить соответствующее значение.
	void addQuery(time_t current_time, const String & quota_name);
	void addError(time_t current_time, const String & quota_name) noexcept;

	/// Проверить, не превышена ли квота уже. Если превышена - кидает исключение.
	void checkExceeded(time_t current_time, const String & quota_name);

	/// Проверить соответствующее значение. Если превышено - кинуть исключение. Иначе - увеличить его.
	void checkAndAddResultRowsBytes(time_t current_time, const String & quota_name, size_t rows, size_t bytes);
	void checkAndAddReadRowsBytes(time_t current_time, const String & quota_name, size_t rows, size_t bytes);
	void checkAndAddExecutionTime(time_t current_time, const String & quota_name, Poco::Timespan amount);

	/// Получить текст, описывающий, какая часть квоты израсходована.
	String toString() const;

	bool operator== (const QuotaForInterval & rhs) const
	{
		return
			rounded_time	== rhs.rounded_time &&
			duration		== rhs.duration &&
			max				== rhs.max &&
			used			== rhs.used;
	}
private:
	/// Сбросить счётчик использованных ресурсов, если соответствующий интервал, за который считается квота, прошёл.
	void updateTime(time_t current_time);
	void check(size_t max_amount, size_t used_amount, time_t current_time, const String & quota_name, const char * resource_name);
};


struct Quota;

/// Length of interval -> quota: maximum allowed and currently accumulated values for that interval (example: 3600 -> values for current hour).
class QuotaForIntervals
{
private:
	/// While checking, will walk through intervals in order of decreasing size - from largest to smallest.
	/// To report first about largest interval on what quota was exceeded.
	using Container = std::map<size_t, QuotaForInterval>;
	Container cont;

	std::string name;

public:
	QuotaForIntervals(const std::string & name_ = "") : name(name_) {}

	/// Is there at least one interval for counting quota?
	bool empty() const
	{
		return cont.empty();
	}

	void initFromConfig(const String & config_elem, Poco::Util::AbstractConfiguration & config, std::mt19937 & rng);

	/// Обновляет максимальные значения значениями из quota.
	/// Удаляет интервалы, которых нет в quota, добавляет интревалы, которых нет здесь, но есть в quota.
	void setMax(const QuotaForIntervals & quota);

	void addQuery(time_t current_time);
	void addError(time_t current_time) noexcept;

	void checkExceeded(time_t current_time);

	void checkAndAddResultRowsBytes(time_t current_time, size_t rows, size_t bytes);
	void checkAndAddReadRowsBytes(time_t current_time, size_t rows, size_t bytes);
	void checkAndAddExecutionTime(time_t current_time, Poco::Timespan amount);

	/// Get text, describing what part of quota has been exceeded.
	String toString() const;

	bool operator== (const QuotaForIntervals & rhs) const
	{
		return cont == rhs.cont && name == rhs.name;
	}
};

using QuotaForIntervalsPtr = std::shared_ptr<QuotaForIntervals>;


/// Quota key -> quotas (max and current values) for intervals. If quota doesn't have keys, then values stored at key 0.
struct Quota
{
	using Container = std::unordered_map<UInt64, QuotaForIntervalsPtr>;

	String name;

	/// Maximum values from config.
	QuotaForIntervals max;
	/// Maximum and accumulated values for different keys.
	/// For all keys, maximum values are the same and taken from 'max'.
	Container quota_for_keys;
	std::mutex mutex;

	bool is_keyed = false;
	bool keyed_by_ip = false;

	void loadFromConfig(const String & config_elem, const String & name_, Poco::Util::AbstractConfiguration & config, std::mt19937 & rng);
	QuotaForIntervalsPtr get(const String & quota_key, const String & user_name, const Poco::Net::IPAddress & ip);
};


class Quotas
{
private:
	/// Name of quota -> quota.
	using Container = std::unordered_map<String, std::unique_ptr<Quota>>;
	Container cont;

public:
	void loadFromConfig(Poco::Util::AbstractConfiguration & config);
	QuotaForIntervalsPtr get(const String & name, const String & quota_key,
							const String & user_name, const Poco::Net::IPAddress & ip);
};

}
