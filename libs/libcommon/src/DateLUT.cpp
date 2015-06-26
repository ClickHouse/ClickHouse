#include <Yandex/DateLUT.h>
#include <Poco/Exception.h>
#include <Yandex/likely.h>
#include <unicode/timezone.h>
#include <unicode/unistr.h>

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

	while ((zone_id != nullptr) && (status == U_ZERO_ERROR))
	{
		std::string zone_id_str;
		zone_id->toUTF8String(zone_id_str);
		date_lut_impl_list.emplace(std::piecewise_construct, std::forward_as_tuple(zone_id_str), std::forward_as_tuple(nullptr));
		zone_id = time_zone_ids->snext(status);
	}
}

DateLUTImpl & DateLUT::instance(const std::string & time_zone)
{
	auto & date_lut = Singleton<DateLUT>::instance();
	return date_lut.get(time_zone);
}

DateLUTImpl & DateLUT::get(const std::string & time_zone)
{
	auto it = date_lut_impl_list.find(time_zone);
	if (it == date_lut_impl_list.end())
		throw Poco::Exception("Invalid time zone " + time_zone);

	auto & wrapper = it->second;

	DateLUTImpl * tmp = wrapper.load(std::memory_order_acquire);
	if (tmp == nullptr)
	{
		std::lock_guard<std::mutex> guard(mux);
		tmp = wrapper.load(std::memory_order_acquire);
		if (tmp == nullptr)
		{
			tmp = new DateLUTImpl(time_zone);
			wrapper.store(tmp, std::memory_order_release);
		}
	}

	return *tmp;
}

