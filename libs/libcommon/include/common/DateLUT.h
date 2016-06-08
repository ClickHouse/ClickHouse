#pragma once

#include <common/DateLUTImpl.h>
#include <common/singleton.h>
#include <Poco/Exception.h>

#include <unordered_map>
#include <vector>
#include <atomic>
#include <mutex>
#include <memory>

/** Этот класс предоставляет метод для того, чтобы создать объект DateLUTImpl
  * для заданного часового пояса, если он не существует.
  */
class DateLUT : public Singleton<DateLUT>
{
	friend class Singleton<DateLUT>;

public:
	DateLUT(const DateLUT &) = delete;
	DateLUT & operator=(const DateLUT &) = delete;

	/// Вернуть единственный экземпляр объекта DateLUTImpl для часового пояса по-умолчанию.
	static __attribute__((__always_inline__)) const DateLUTImpl & instance()
	{
		const auto & date_lut = Singleton<DateLUT>::instance();
		return *date_lut.default_date_lut_impl;
	}

	/// Вернуть единственный экземпляр объекта DateLUTImpl для заданного часового пояса.
	static __attribute__((__always_inline__)) const DateLUTImpl & instance(const std::string & time_zone)
	{
		const auto & date_lut = Singleton<DateLUT>::instance();
		return date_lut.get(time_zone);
	}

public:
	/// Отображение часового пояса в группу эквивалентных часовый поясов.
	/// Два часовых пояса эквивалентные, если они обладают одними и теми же свойствами.
	using TimeZoneToGroup = std::unordered_map<std::string, size_t>;
	/// Хранилище для lookup таблиц DateLUTImpl.
	using DateLUTImplList = std::vector<std::atomic<DateLUTImpl *> >;

protected:
	DateLUT();

private:
	__attribute__((__always_inline__)) const DateLUTImpl & get(const std::string & time_zone) const
	{
		if (time_zone.empty())
			return *default_date_lut_impl;

		auto it = time_zone_to_group.find(time_zone);
		if (it == time_zone_to_group.end())
			throw Poco::Exception("Invalid time zone " + time_zone);

		const auto & group_id = it->second;
		if (group_id == default_group_id)
			return *default_date_lut_impl;

		return getImplementation(time_zone, group_id);
	}

	const DateLUTImpl & getImplementation(const std::string & time_zone, size_t group_id) const;

private:
	/// Указатель на реализацию для часового пояса по-умолчанию.
	std::unique_ptr<DateLUTImpl> default_date_lut_impl;
	/// Соответствующиая группа часовых поясов по-умолчанию.
	size_t default_group_id;
	///
	TimeZoneToGroup time_zone_to_group;
	/// Lookup таблица для каждой группы часовых поясов.
	mutable std::unique_ptr<DateLUTImplList> date_lut_impl_list;
	mutable std::mutex mutex;
};
