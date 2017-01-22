#include <common/DateLUT.h>

#include <Poco/Exception.h>

#pragma GCC diagnostic push
#ifdef __APPLE__
#pragma GCC diagnostic ignored "-Wold-style-cast"
#endif
#include <unicode/timezone.h>
#include <unicode/unistr.h>
#pragma GCC diagnostic pop


DateLUT::DateLUT()
{
	/// Initialize the pointer to the default DateLUTImpl.

	std::unique_ptr<icu::TimeZone> tz(icu::TimeZone::createDefault());
	if (tz == nullptr)
		throw Poco::Exception("Failed to determine the host time zone.");

	icu::UnicodeString u_out;
	tz->getID(u_out);
	std::string default_time_zone;
	u_out.toUTF8String(default_time_zone);

	default_impl.store(&getImplementation(default_time_zone), std::memory_order_release);
}


const DateLUTImpl & DateLUT::getImplementation(const std::string & time_zone) const
{
	std::lock_guard<std::mutex> lock(mutex);

	auto it = impls.emplace(time_zone, nullptr).first;
	if (!it->second)
		it->second = std::make_unique<DateLUTImpl>(time_zone);

	return *it->second;
}
