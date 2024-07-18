#include <DataTypes/DataTypeDecimalBase.h>
#include <Interpreters/Context.h>
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

template <is_decimal T>
Field DataTypeDecimalBase<T>::getDefault() const
{
    return DecimalField(T(0), scale);
}

template <is_decimal T>
MutableColumnPtr DataTypeDecimalBase<T>::createColumn() const
{
    return ColumnType::create(0, scale);
}

template <is_decimal T>
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
