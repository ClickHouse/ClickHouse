#pragma once

#include <functional>
#include <common/Types.h>
#include <common/strong_typedef.h>

/// The following is for Yandex.Metrika code. Not used by ClickHouse.


/** Тип данных для хранения идентификатора пользователя. */
using UserID_t = UInt64;

/** Тип данных для хранения идентификатора счетчика. */
using CounterID_t = UInt32;

/** Идентификатор хита */
using WatchID_t = UInt64;

/** Идентификатор визита */
STRONG_TYPEDEF(UInt64, VisitID_t);

namespace std
{
    template <> struct is_integral<VisitID_t> : std::true_type {};
    template <> struct is_arithmetic<VisitID_t> : std::true_type {};
}

/** Идентификатор клика */
using ClickID_t = UInt64;

/** Идентификатор цели */
using GoalID_t = UInt32;
