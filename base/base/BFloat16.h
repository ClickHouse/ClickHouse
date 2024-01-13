#pragma once

using BFloat16 = __bf16;

namespace std
{
    inline constexpr bool isfinite(BFloat16) { return true; }
    inline constexpr bool signbit(BFloat16) { return false; }
}
