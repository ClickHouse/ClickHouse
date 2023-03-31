#pragma once
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

const NameAndTypePair BlockNumberColumn {"_block_number", std::make_shared<DataTypeUInt64>()};

}
