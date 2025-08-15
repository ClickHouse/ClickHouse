#include <Core/MySQL/MySQLUtils.h>

#include <Common/assert_cast.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsDateTime.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/IDataType.h>
#include <base/types.h>

namespace DB
{
namespace MySQLProtocol
{
namespace MySQLUtils
{

DecimalUtils::DecimalComponents<DateTime64> getNormalizedDateTime64Components(DataTypePtr data_type, ColumnPtr col, size_t row_num)
{
    const auto * date_time_type = typeid_cast<const DataTypeDateTime64 *>(data_type.get());

    static constexpr UInt32 MaxScale = DecimalUtils::max_precision<DateTime64>;
    UInt32 scale = std::min(MaxScale, date_time_type->getScale());

    const auto value = assert_cast<const ColumnDateTime64 &>(*col).getData()[row_num];
    auto components = DecimalUtils::split(value, scale);

    using T = typename DateTime64::NativeType;
    if (value.value < 0 && components.fractional)
    {
        components.fractional = DecimalUtils::scaleMultiplier<T>(scale) + (components.whole ? T(-1) : T(1)) * components.fractional;
        --components.whole;
    }

    if (components.fractional != 0)
    {
        if (scale > 6)
        {
            // MySQL Timestamp has max scale of 6
            components.fractional /= static_cast<int>(pow(10, scale - 6));
        }
        else
        {
            // fractional == 1 is a different microsecond value depending on the scale
            // Scale 1 = 100000
            // Scale 2 = 010000
            // Scale 3 = 001000
            // Scale 4 = 000100
            // Scale 5 = 000010
            // Scale 6 = 000001
            components.fractional *= static_cast<int>(pow(10, 6 - scale));
        }
    }

    return components;
};
}
}
}
