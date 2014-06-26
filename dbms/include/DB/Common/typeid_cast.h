#pragma once

#include <type_traits>
#include <typeinfo>


/** Проверяет совпадение типа путём сравнения typeid-ов.
  * Проверяется точное совпадение типа. То есть, cast в предка будет неуспешным.
  * В остальном, ведёт себя как dynamic_cast.
  */
template <typename To, typename From>
typename std::enable_if<std::is_reference<To>::value, To>::type typeid_cast(From & from)
{
	if (typeid(from) == typeid(To))
		return static_cast<To>(from);
	else
		throw std::bad_cast();
}

template <typename To, typename From>
To typeid_cast(From * from)
{
	if (typeid(*from) == typeid(typename std::remove_pointer<To>::type))
		return static_cast<To>(from);
	else
		return nullptr;
}
