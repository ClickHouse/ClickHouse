#include <Yandex/DateLUT.h>
#include <Poco/Exception.h>

#include <unicode/timezone.h>
#include <unicode/unistr.h>
#include <memory>

std::string DateLUT::default_time_zone;

DateLUT::DateLUT()
{
	using namespace icu;

	std::unique_ptr<TimeZone> tz(TimeZone::createDefault());
	if (tz == nullptr)
		throw Poco::Exception("Failed to query the host time zone.");

	UnicodeString u_out;
	tz->getID(u_out);
	u_out.toUTF8String(default_time_zone);

	std::unique_ptr<StringEnumeration> time_zone_ids(TimeZone::createEnumeration());
	if (time_zone_ids == nullptr)
		throw Poco::Exception("Failed to query the list of time zones.");

	UErrorCode status = U_ZERO_ERROR;
	const UnicodeString * zone_id = time_zone_ids->snext(status);
	if (zone_id == nullptr)
		throw Poco::Exception("No time zone available.");

	std::vector<UnicodeString> time_zones;
	while ((zone_id != nullptr) && (status == U_ZERO_ERROR))
	{
		time_zones.push_back(*zone_id);
		zone_id = time_zone_ids->snext(status);
	}

	for (const auto & time_zone : time_zones)
	{
		const UnicodeString & u_group_id = TimeZone::getEquivalentID(time_zone, 0);
		std::string group_id;

		if (u_group_id.isEmpty())
		{
			time_zone.toUTF8String(group_id);

			auto res = time_zone_to_group.insert(std::make_pair(group_id, group_id));
			if (!res.second)
				throw Poco::Exception("Failed to initialize time zone information.");
			auto res2 = date_lut_impl_list.emplace(std::piecewise_construct, std::forward_as_tuple(group_id), std::forward_as_tuple(nullptr));
			if (!res2.second)
				throw Poco::Exception("Failed to initialize time zone information.");
		}
		else
		{
			u_group_id.toUTF8String(group_id);

			auto it = time_zone_to_group.find(group_id);
			if (it == time_zone_to_group.end())
			{
				auto count = TimeZone::countEquivalentIDs(time_zone);
				for (auto i = 0; i < count; ++i)
				{
					const UnicodeString & u_equivalent_id = TimeZone::getEquivalentID(time_zone, i);
					std::string equivalent_id;
					u_equivalent_id.toUTF8String(equivalent_id);
					auto res = time_zone_to_group.insert(std::make_pair(equivalent_id, group_id));
					if (!res.second)
						throw Poco::Exception("Failed to initialize time zone information.");
				}
				auto res = date_lut_impl_list.emplace(std::piecewise_construct, std::forward_as_tuple(group_id), std::forward_as_tuple(nullptr));
				if (!res.second)
					throw Poco::Exception("Failed to initialize time zone information.");
			}
		}
	}
}

DateLUTImpl & DateLUT::instance(const std::string & time_zone)
{
	auto & date_lut = Singleton<DateLUT>::instance();
	return date_lut.get(time_zone);
}

DateLUTImpl & DateLUT::get(const std::string & time_zone)
{
	auto it = time_zone_to_group.find(time_zone);
	if (it == time_zone_to_group.end())
		throw Poco::Exception("Invalid time zone " + time_zone);
	const auto & group_id = it->second;

	auto it2 = date_lut_impl_list.find(group_id);
	if (it2 == date_lut_impl_list.end())
		throw Poco::Exception("Invalid group of equivalent time zones.");

	auto & wrapper = it2->second;

	DateLUTImpl * tmp = wrapper.load(std::memory_order_acquire);
	if (tmp == nullptr)
	{
		std::lock_guard<std::mutex> guard(mutex);
		tmp = wrapper.load(std::memory_order_acquire);
		if (tmp == nullptr)
		{
			tmp = new DateLUTImpl(group_id);
			wrapper.store(tmp, std::memory_order_release);
		}
	}

	return *tmp;
}

