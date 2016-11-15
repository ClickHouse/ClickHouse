#pragma once

#include <common/DateLUTImpl.h>
#include <common/singleton.h>
#include <DB/Core/Defines.h>
#include <Poco/Exception.h>

#include <unordered_map>
#include <vector>
#include <atomic>
#include <mutex>
#include <memory>

/// This class provides lazy initialization and lookup of singleton DateLUTImpl objects for a given timezone.
class DateLUT : public Singleton<DateLUT>
{
	friend class Singleton<DateLUT>;

public:
	DateLUT(const DateLUT &) = delete;
	DateLUT & operator=(const DateLUT &) = delete;

	/// Return singleton DateLUTImpl instance for the default time zone.
	static ALWAYS_INLINE const DateLUTImpl & instance()
	{
		const auto & date_lut = Singleton<DateLUT>::instance();
		return *date_lut.default_impl.load(std::memory_order_acquire);
	}

	/// Return singleton DateLUTImpl instance for a given time zone.
	static ALWAYS_INLINE const DateLUTImpl & instance(const std::string & time_zone)
	{
		const auto & date_lut = Singleton<DateLUT>::instance();
		if (time_zone.empty())
			return *date_lut.default_impl.load(std::memory_order_acquire);

		return date_lut.getImplementation(time_zone);
	}

	static void setDefaultTimezone(const std::string & time_zone)
	{
		auto & date_lut = Singleton<DateLUT>::instance();
		const auto & impl = date_lut.getImplementation(time_zone);
		date_lut.default_impl.store(&impl, std::memory_order_release);
	}

protected:
	DateLUT();

private:
	ALWAYS_INLINE const DateLUTImpl & getImplementation(const std::string & time_zone) const
	{
		auto it = time_zone_to_group.find(time_zone);
		if (it == time_zone_to_group.end())
			throw Poco::Exception("Invalid time zone " + time_zone);
		size_t group_id = it->second;

		auto initialize_impl = [this, group_id, &time_zone]()
		{
			date_lut_impls[group_id] = std::make_unique<DateLUTImpl>(time_zone);
		};

		std::call_once(initialized_flags[group_id], initialize_impl);
		return *date_lut_impls[group_id];
	}

private:
	/// A mapping of a time zone name into a group id of equivalent time zones.
	/// Two time zones are considered equivalent if they have the same properties.
	using TimeZoneToGroup = std::unordered_map<std::string, size_t>;

	TimeZoneToGroup time_zone_to_group;

	using DateLUTImplPtr = std::unique_ptr<DateLUTImpl>;

	/// A vector of lookup tables indexed by group id and their initialization flags.
	mutable std::vector<DateLUTImplPtr> date_lut_impls;
	mutable std::vector<std::once_flag> initialized_flags;

	std::atomic<const DateLUTImpl *> default_impl;
};
