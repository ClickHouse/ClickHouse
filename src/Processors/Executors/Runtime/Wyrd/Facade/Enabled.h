#pragma once

namespace DB::Runtime::Wyrd
{

constexpr bool enabled()
{
#if defined(WYRD_SIMULATION) || defined(WYRD_ADVERSARY)
    return true;
#else
    return false;
#endif
}

}
