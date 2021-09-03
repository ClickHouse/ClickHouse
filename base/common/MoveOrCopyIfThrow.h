#pragma once

#include <common/types.h>

namespace detail
{
    template <typename T, bool is_nothrow_move_assignable = std::is_nothrow_move_assignable_v<T>>
    struct MoveOrCopyIfThrow;

    template <typename T>
    struct MoveOrCopyIfThrow<T, true>
    {
        void operator()(T && src, T & dst) const
        {
            dst = std::forward<T>(src);
        }
    };

    template <typename T>
    struct MoveOrCopyIfThrow<T, false>
    {
        void operator()(T && src, T & dst) const
        {
            dst = src;
        }
    };

    template <typename T>
    void moveOrCopyIfThrow(T && src, T & dst)
    {
        MoveOrCopyIfThrow<T>()(std::forward<T>(src), dst);
    }
}
