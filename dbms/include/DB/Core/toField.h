#pragma once

#include <common/Common.h>		/// VisitID_t
#include <common/LocalDate.h>
#include <common/LocalDateTime.h>
#include <DB/Core/Field.h>
#include <mysqlxx/Null.h>


namespace DB
{


/// Перевести что угодно в Field.
template <typename T>
inline Field toField(const T & x)
{
	return Field(typename NearestFieldType<T>::Type(x));
}

inline Field toField(const LocalDate & x)
{
	return toField(static_cast<UInt16>(x.getDayNum()));
}

inline Field toField(const LocalDateTime & x)
{
	return toField(static_cast<UInt32>(static_cast<time_t>(x)));
}

inline Field toField(const VisitID_t & x)
{
	return toField(static_cast<UInt64>(x));
}

template <typename T>
inline Field toField(const mysqlxx::Null<T> & x)
{
	return x.isNull() ? Field(Null()) : toField(static_cast<const T &>(x));
}


}
