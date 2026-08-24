#pragma once

#include <Core/Types.h>

#include <cstdint>
#include <memory>

namespace DB
{

template <typename Type>
class DataTypeEnum;
using DataTypeEnum8 = DataTypeEnum<Int8>;

// Make it signed for compatibility with DataTypeEnum8
// The values carry no ordering. PRIVATE_PREVIEW is the largest one, which getTier relies on.
enum SettingsTierType : int8_t
{
    PRODUCTION = 0b00000,
    OBSOLETE = 0b00100,
    EXPERIMENTAL = 0b01000,
    PRIVATE_PREVIEW = 0b10000,
    BETA = 0b01100
};

std::shared_ptr<DataTypeEnum8> getSettingsTierEnum();

}
