#pragma once

namespace DB
{

/// Some window functions support adding [IGNORE|RESPECT] NULLS
enum class NullsAction : UInt8
{
    EMPTY,
    RESPECT_NULLS,
    IGNORE_NULLS,
};

}
