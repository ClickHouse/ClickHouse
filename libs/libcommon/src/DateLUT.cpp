#include <common/DateLUT.h>

#include <unicode/timezone.h>
#include <unicode/unistr.h>

DateLUT::DateLUT()
{
	using namespace icu;

	std::unique_ptr<TimeZone> tz(TimeZone::createDefault());
	if (tz == nullptr)
		throw Poco::Exception("Failed to determine the host time zone.");

	UnicodeString u_out;
	tz->getID(u_out);
	std::string default_time_zone;
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

	size_t group_id = 0;

	for (const auto & time_zone : time_zones)
	{
		const UnicodeString & u_group_name = TimeZone::getEquivalentID(time_zone, 0);
		std::string group_name;

		if (u_group_name.isEmpty())
		{
			time_zone.toUTF8String(group_name);

			auto res = time_zone_to_group.insert(std::make_pair(group_name, group_id));
			if (!res.second)
				throw Poco::Exception("Failed to initialize time zone information.");
			++group_id;
		}
		else
		{
			u_group_name.toUTF8String(group_name);

			auto it = time_zone_to_group.find(group_name);
			if (it == time_zone_to_group.end())
			{
				auto count = TimeZone::countEquivalentIDs(time_zone);
				if (count == 0)
					throw Poco::Exception("Inconsistent time zone information.");

				for (auto i = 0; i < count; ++i)
				{
					const UnicodeString & u_name = TimeZone::getEquivalentID(time_zone, i);
					std::string name;
					u_name.toUTF8String(name);
					auto res = time_zone_to_group.insert(std::make_pair(name, group_id));
					if (!res.second)
						throw Poco::Exception("Failed to initialize time zone information.");
				}
				++group_id;
			}
		}
	}

	if (group_id == 0)
		throw Poco::Exception("Could not find any time zone information.");

	date_lut_impl_list = std::make_unique<DateLUTImplList>(group_id);

	/// Инициализация указателя на реализацию для часового пояса по-умолчанию.
	auto it = time_zone_to_group.find(default_time_zone);
	if (it == time_zone_to_group.end())
		throw Poco::Exception("Failed to get default time zone information.");
	default_group_id = it->second;

	default_date_lut_impl.reset(new DateLUTImpl(default_time_zone));
	auto & wrapper = (*date_lut_impl_list)[default_group_id];
	wrapper.store(default_date_lut_impl.get(), std::memory_order_seq_cst);
}

const DateLUTImpl & DateLUT::getImplementation(const std::string & time_zone, size_t group_id) const
{
	auto & wrapper = (*date_lut_impl_list)[group_id];

	DateLUTImpl * tmp = wrapper.load(std::memory_order_acquire);
	if (tmp == nullptr)
	{
		std::lock_guard<std::mutex> guard(mutex);
		tmp = wrapper.load(std::memory_order_relaxed);
		if (tmp == nullptr)
		{
			tmp = new DateLUTImpl(time_zone);
			wrapper.store(tmp, std::memory_order_release);
		}
	}

	return *tmp;
}
