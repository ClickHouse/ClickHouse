#pragma once

namespace DB::Wyrd
{

constexpr bool enabled()
{
#if defined(WYRD_ENABLED)
    return true;
#else
    return false;
#endif
}

}
