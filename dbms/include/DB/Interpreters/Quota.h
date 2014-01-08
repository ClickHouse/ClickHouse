#pragma once

#include <string.h>
#include <unordered_map>

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

	void initFromConfig(const String & config_elem);
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

	void initFromConfig(const String & config_elem, time_t duration_);

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

	Quota * parent;

public:
	QuotaForIntervals(Quota * parent_) : parent(parent_) {}

	/// Есть ли хотя бы один интервал, за который считается квота?
	bool empty() const
	{
		return cont.empty();
	}
	
	void initFromConfig(const String & config_elem);

	void addQuery(time_t current_time);
	void addError(time_t current_time);

	void checkExceeded(time_t current_time);

	void checkAndAddResultRowsBytes(time_t current_time, size_t rows, size_t bytes);
	void checkAndAddReadRowsBytes(time_t current_time, size_t rows, size_t bytes);
	void checkAndAddExecutionTime(time_t current_time, Poco::Timespan amount);

	/// Получить текст, описывающий, какая часть квоты израсходована.
	String toString() const;
};


/// Ключ квоты -> квоты за интервалы. Если квота не допускает ключей, то накопленные значения хранятся по ключу 0.
struct Quota
{
	typedef std::unordered_map<UInt64, QuotaForIntervals> Container;

	String name;

	/// Максимальные значения из конфига.
	QuotaForIntervals max;
	/// Максимальные и накопленные значения для разных ключей.
	Container quota_for_keys;
	Poco::FastMutex mutex;

	bool is_keyed;
	bool keyed_by_ip;

	Quota() : max(this), is_keyed(false), keyed_by_ip(false) {}

	void initFromConfig(const String & config_elem, const String & name_);
	QuotaForIntervals & get(const String & quota_key, const String & user_name, const Poco::Net::IPAddress & ip);
};


class Quotas
{
private:
	/// Имя квоты -> квоты.
	typedef std::unordered_map<String, SharedPtr<Quota> > Container;
	Container cont;

public:
	void initFromConfig();
	QuotaForIntervals & get(const String & name, const String & quota_key, const String & user_name, const Poco::Net::IPAddress & ip);
};

}
