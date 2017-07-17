#pragma once

#include <common/LocalDate.h>
#include <common/LocalDateTime.h>
#include <Core/Field.h>


namespace DB
{

/// Transform anything to Field.
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

}
