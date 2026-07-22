#pragma once

#include <base/types.h>

namespace DB
{

/// Positions layout marker persisted in the text index header (one byte per part). Only 'Blocked'
/// is supported (not user-configurable); 0/1 are pre-release roaringish layouts, so such parts must
/// be dropped and re-created.
class TextIndexPositionCodec
{
public:
    enum class Encoding : UInt8
    {
        LegacyRaw = 0,
        LegacyPfor = 1,
        Blocked = 2,
    };
};

}
