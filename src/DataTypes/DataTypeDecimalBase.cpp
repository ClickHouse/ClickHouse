#include <type_traits>
#include <Core/DecimalFunctions.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeDecimalBase.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool decimal_check_overflow;
}

namespace ErrorCodes
{
}

template <is_decimal T>
constexpr size_t DataTypeDecimalBase<T>::maxPrecision()
{
    return DecimalUtils::max_precision<T>;
}

bool decimalCheckComparisonOverflow(ContextPtr context)
{
    return context->getSettingsRef()[Setting::decimal_check_overflow];
}
bool decimalCheckArithmeticOverflow(ContextPtr context)
{
    return context->getSettingsRef()[Setting::decimal_check_overflow];
}

template <is_decimal T>
Field DataTypeDecimalBase<T>::getDefault() const
{
    return DecimalField<T>(T(0), scale);
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

template <is_decimal T>
T DataTypeDecimalBase<T>::wholePart(T x) const
{
    return DecimalUtils::getWholePart(x, scale);
}

template <is_decimal T>
T DataTypeDecimalBase<T>::fractionalPart(T x) const
{
    return DecimalUtils::getFractionalPart(x, scale);
}


/// Explicit template instantiations.
template class DataTypeDecimalBase<Decimal32>;
template class DataTypeDecimalBase<Decimal64>;
template class DataTypeDecimalBase<Decimal128>;
template class DataTypeDecimalBase<Decimal256>;
template class DataTypeDecimalBase<DateTime64>;
template class DataTypeDecimalBase<Time64>;

}
