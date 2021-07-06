#include <DataTypes/DataTypeDecimalBase.h>

#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypeFactory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST.h>

#include <type_traits>

namespace DB
{

namespace ErrorCodes
{
}

bool decimalCheckComparisonOverflow(ContextPtr context)
{
    return context->getSettingsRef().decimal_check_overflow;
}
bool decimalCheckArithmeticOverflow(ContextPtr context)
{
    return context->getSettingsRef().decimal_check_overflow;
}

template <typename T>
Field DataTypeDecimalBase<T>::getDefault() const
{
    return DecimalField(T(0), scale);
}

template <typename T>
MutableColumnPtr DataTypeDecimalBase<T>::createColumn() const
{
    return ColumnType::create(0, scale);
}

template <typename T>
T DataTypeDecimalBase<T>::getScaleMultiplier(UInt32 scale_)
{
    return DecimalUtils::scaleMultiplier<typename T::NativeType>(scale_);
}


/// Explicit template instantiations.
template class DataTypeDecimalBase<Decimal32>;
template class DataTypeDecimalBase<Decimal64>;
template class DataTypeDecimalBase<Decimal128>;
template class DataTypeDecimalBase<Decimal256>;
template class DataTypeDecimalBase<DateTime64>;

}
