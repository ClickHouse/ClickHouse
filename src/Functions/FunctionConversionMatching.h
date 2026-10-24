#pragma once

namespace DB
{

class IFunctionBase;

/// Checks whether two CAST bases have equivalent semantics for Bloom-filter expression matching.
/// Unsupported conversions must continue to use the complete IFunctionBase::updateHash value.
bool areCastsEquivalentForBloomFilterExpressionMatching(const IFunctionBase & left, const IFunctionBase & right);

/// Authenticates the native final CAST implementation before inspecting its conversion types.
/// This checks implementation identity only; callers must prove the required semantics separately.
bool isNativeCastForBloomFilterExpressionMatching(const IFunctionBase & function);

/// Authenticates the exact native conversions that consult cast_string_to_date_time_mode
/// while parsing String/FixedString input. Callers must still validate source types.
bool isNativeDateTimeModeDependentConversionForBloomFilterExpressionMatching(const IFunctionBase & function);

/// Authenticates the native CAST and exact native FunctionConvert/FunctionConvertFromString
/// families that capture FunctionConvertSettings, without invoking function virtual hooks.
bool isNativeSettingsCapturingConversionForBloomFilterExpressionMatching(const IFunctionBase & function);

/// Checks the audited native integer-to-UInt8, numeric-to-Decimal and numeric/Decimal-to-String conversions whose values do
/// not depend on captured conversion settings. Unsupported conversions must continue to use the
/// complete IFunctionBase::updateHash value. Callers remain responsible for comparing constant
/// argument values.
bool areNativeConversionsEquivalentForBloomFilterExpressionMatching(const IFunctionBase & left, const IFunctionBase & right);

}
