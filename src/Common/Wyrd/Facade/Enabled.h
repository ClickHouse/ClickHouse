#pragma once

namespace DB::Wyrd
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
