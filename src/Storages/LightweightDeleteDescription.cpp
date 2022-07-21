#include <Storages/LightweightDeleteDescription.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

const NameAndTypePair LightweightDeleteDescription::filter_column {"_row_exists", std::make_shared<DataTypeUInt8>()};

}
