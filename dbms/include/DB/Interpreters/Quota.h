#pragma once

#include <string.h>
#include <unordered_map>
#include <memory>

#include <Poco/Timespan.h>

#include <Poco/Util/Application.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <Poco/Net/IPAddress.h>

#include <Yandex/Common.h>

#include <DB/Core/Types.h>
#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/IO/WriteHelpers.h>


namespace DB
{

/** Квота на потребление ресурсов за заданный интервал - часть настроек.
  * Используются, чтобы ограничить потребление ресурсов пользователем.
  * Квота действует "мягко" - может быть немного превышена, так как проверяется, как правило, на каждый блок.
  * Квота не сохраняется при перезапуске сервера.
  * Квота распространяется только на один сервер.
  */

/// Используется как для максимальных значений, так и в качестве счётчика накопившихся значений.
struct QuotaValues
{
	/// Нули в качестве ограничений означают "не ограничено".
	size_t queries;					/// Количество запросов.
	size_t errors;					/// Количество запросов с эксепшенами.
	size_t result_rows;				/// Количество строк, отданных в качестве результата.
	size_t result_bytes;			/// Количество байт, отданных в качестве результата.
	size_t read_rows;				/// Количество строк, прочитанных из таблиц.
	size_t read_bytes;				/// Количество байт, прочитанных из таблиц.
	Poco::Timespan execution_time;	/// Суммарное время выполнения запросов.

	QuotaValues()
	{
		clear();
	}

	void clear()
	{
		memset(this, 0, sizeof(*this));
	}

	void initFromConfig(const String & config_elem, Poco::Util::AbstractConfiguration & config);

	bool operator== (const QuotaValues & rhs) const
	{
		return
			queries			== rhs.queries &&
			errors			== rhs.errors &&
			result_rows		== rhs.result_rows &&
			result_bytes	== rhs.result_bytes &&
			read_rows		== rhs.read_rows &&
			read_bytes		== rhs.read_bytes &&
			execution_time	== rhs.execution_time;
	}
};


/// Время, округлённое до границы интервала, квота за интервал и накопившиеся за этот интервал значения.
struct QuotaForInterval
{
	time_t rounded_time;
	size_t duration;
	QuotaValues max;
	QuotaValues used;

	QuotaForInterval() : rounded_time() {}
	QuotaForInterval(time_t duration_) : duration(duration_) {}

	void initFromConfig(const String & config_elem, time_t duration_, Poco::Util::AbstractConfiguration & config);

	/// Увеличить соответствующее значение.
	void addQuery(time_t current_time, const String & quota_name);
	void addError(time_t current_time, const String & quota_name);

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

/// Длина интервала -> квота и накопившиеся за текущий интервал такой длины значения (например, 3600 -> значения за текущий час).
class QuotaForIntervals
{
private:
	/// При проверке, будем обходить интервалы в порядке, обратном величине - от самого большого до самого маленького.
	typedef std::map<size_t, QuotaForInterval> Container;
	Container cont;

	std::string name;

public:
	QuotaForIntervals(const std::string & name_ = "") : name(name_) {}

	/// Есть ли хотя бы один интервал, за который считается квота?
	bool empty() const
	{
		return cont.empty();
	}

	void initFromConfig(const String & config_elem, Poco::Util::AbstractConfiguration & config);

	/// Обновляет максимальные значения значениями из quota.
	/// Удаляет интервалы, которых нет в quota, добавляет интревалы, которых нет здесь, но есть в quota.
	void setMax(const QuotaForIntervals & quota);

	void addQuery(time_t current_time);
	void addError(time_t current_time);

	void checkExceeded(time_t current_time);

	void checkAndAddResultRowsBytes(time_t current_time, size_t rows, size_t bytes);
	void checkAndAddReadRowsBytes(time_t current_time, size_t rows, size_t bytes);
	void checkAndAddExecutionTime(time_t current_time, Poco::Timespan amount);

	/// Получить текст, описывающий, какая часть квоты израсходована.
	String toString() const;

	bool operator== (const QuotaForIntervals & rhs) const
	{
		return cont == rhs.cont && name == rhs.name;
	}
};

typedef std::shared_ptr<QuotaForIntervals> QuotaForIntervalsPtr;


/// Ключ квоты -> квоты за интервалы. Если квота не допускает ключей, то накопленные значения хранятся по ключу 0.
struct Quota
{
	typedef std::unordered_map<UInt64, QuotaForIntervalsPtr> Container;

	String name;

	/// Максимальные значения из конфига.
	QuotaForIntervals max;
	/// Максимальные и накопленные значения для разных ключей.
	/// Для всех ключей максимальные значения одинаковы и взяты из max.
	Container quota_for_keys;
	Poco::FastMutex mutex;

	bool is_keyed;
	bool keyed_by_ip;

	Quota() : is_keyed(false), keyed_by_ip(false) {}

	void loadFromConfig(const String & config_elem, const String & name_, Poco::Util::AbstractConfiguration & config);
	QuotaForIntervalsPtr get(const String & quota_key, const String & user_name, const Poco::Net::IPAddress & ip);
};


class Quotas
{
private:
	/// Имя квоты -> квоты.
	typedef std::unordered_map<String, SharedPtr<Quota> > Container;
	Container cont;

public:
	void loadFromConfig(Poco::Util::AbstractConfiguration & config);
	QuotaForIntervalsPtr get(const String & name, const String & quota_key,
							const String & user_name, const Poco::Net::IPAddress & ip);
};

}
