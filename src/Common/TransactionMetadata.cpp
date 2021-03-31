#include <Common/TransactionMetadata.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>

namespace DB
{

DataTypePtr TransactionID::getDataType()
{
    DataTypes types;
    types.push_back(std::make_shared<DataTypeUInt64>());
    types.push_back(std::make_shared<DataTypeUInt64>());
    types.push_back(std::make_shared<DataTypeUUID>());
    return std::make_shared<DataTypeTuple>(std::move(types));
}

}
