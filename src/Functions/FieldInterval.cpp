#include <Functions/FieldInterval.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>

namespace DB
{

bool canCalculatePreimageForConstant(const DataTypePtr & result_type, const DataTypePtr & constant_type)
{
    /// Wrappers keep the value intact, and the comparison looks through them as well.
    const WhichDataType result(removeNullable(removeLowCardinality(result_type)));
    const WhichDataType constant(removeNullable(removeLowCardinality(constant_type)));
    return result.idx == constant.idx
        || !result.isDateOrDate32OrTimeOrTime64OrDateTimeOrDateTime64()
        || !constant.isDateOrDate32OrTimeOrTime64OrDateTimeOrDateTime64();
}

}
