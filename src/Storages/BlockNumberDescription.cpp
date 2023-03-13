#include <Storages/BlockNumberDescription.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

const NameAndTypePair BlockNumberDescription::COLUMN {"_block_number", std::make_shared<DataTypeUInt64>()};

}
