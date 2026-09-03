#pragma once

#include <base/types.h>

namespace DB
{

/// Codec of the positions stream (.pos), persisted in the text index header (one byte per part) so every part
/// self-describes its positions encoding and future codecs can be added without breaking readers.
class TextIndexPositionCodec
{
public:
    enum class Encoding : UInt8
    {
        BlockedPfor = 1,
    };
};

}
