#include <DataTypes/DataTypeExponentialTimeDecayingFloat64.h>

#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>

#include <utility>

namespace DB
{

namespace
{

std::pair<DataTypePtr, DataTypeCustomDescPtr> create()
{
    auto storage_type = std::make_shared<DataTypeTuple>(
        DataTypes{
            std::make_shared<DataTypeFloat64>(),
            std::make_shared<DataTypeFloat64>(),
            std::make_shared<DataTypeFloat64>()},
        Names{"value", "time", "half_life"});

    return {
        storage_type,
        std::make_unique<DataTypeCustomDesc>(
            std::make_unique<DataTypeCustomFixedName>("ExponentialTimeDecayingFloat64"))};
}

}

DataTypePtr createDataTypeExponentialTimeDecayingFloat64()
{
    return DataTypeFactory::instance().getCustom(
        "Tuple(value Float64, time Float64, half_life Float64)",
        std::make_unique<DataTypeCustomDesc>(
            std::make_unique<DataTypeCustomFixedName>("ExponentialTimeDecayingFloat64")));
}

bool isExponentialTimeDecayingFloat64(const DataTypePtr & type)
{
    return type && type->getCustomName() && type->getCustomName()->getName() == "ExponentialTimeDecayingFloat64";
}

void registerDataTypeExponentialTimeDecayingFloat64(DataTypeFactory & factory)
{
    factory.registerSimpleDataTypeCustom(
        "ExponentialTimeDecayingFloat64",
        create,
        DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = R"(
Represents one or more non-negative exponentially time-decaying values at a shared anchor time.

The fields are `value`, `time`, and `half_life`, all stored as `Float64`. DateTime and DateTime64
inputs are represented as seconds. Values can only be added when their half-lives are identical.
Use `tupleElement(decaying_value, 'time')` to read the greatest observed or current anchor time.
)",
            .syntax = "ExponentialTimeDecayingFloat64",
            .examples = {},
            .related = {},
        });
}

}
