#pragma once

#include <DB/Common/BaseCellAging.h>
#include <chrono>

namespace DB
{

/** Класс для управления временем жизнью элементов кэша.
  */
class CellAging final : public BaseCellAging<std::chrono::steady_clock::time_point, std::chrono::seconds>
{
public:
	const Timestamp & update() override
	{
		timestamp = std::chrono::steady_clock::now();
		return timestamp;
	}

	bool expired(const Timestamp & last_timestamp, const Delay & expiration_delay) const override
	{
		return (last_timestamp > timestamp) && ((last_timestamp - timestamp) > expiration_delay);
	}
};

}
