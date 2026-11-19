#pragma once

#include <Common/Wyrd/Namespace.h>

namespace WYRD_NAMESPACE(DB::Wyrd)
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
