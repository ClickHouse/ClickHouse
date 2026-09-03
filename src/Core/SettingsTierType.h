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
enum SettingsTierType : int8_t
{
    PRODUCTION = 0b0000,
    OBSOLETE = 0b0100,
    EXPERIMENTAL = 0b1000,
    BETA = 0b1100
};

std::shared_ptr<DataTypeEnum8> getSettingsTierEnum();

}
