#pragma once

namespace DB
{

/** Базовый класс для управления временем жизнью элементов кэша.
  */
template <typename TTimestamp, typename TDelay>
class BaseCellAging
{
public:
	using Timestamp = TTimestamp;
	using Delay = TDelay;

public:
	virtual ~BaseCellAging() = default;
	/// Обновить timestamp элемента кэша.
	virtual const Timestamp & update() = 0;
	/// Просрочен ли элемент кэша? Срок истечения годности задается в секундах.
	virtual bool expired(const Timestamp & last_timestamp, const Delay & expiration_delay) const = 0;

protected:
	Timestamp timestamp = Timestamp();
};

}
