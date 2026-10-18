#pragma once

namespace DB
{

class IFunctionBase;

/// Checks whether two CAST bases have equivalent semantics for Bloom-filter expression matching.
/// Unsupported conversions must continue to use the complete IFunctionBase::updateHash value.
bool areCastsEquivalentForBloomFilterExpressionMatching(const IFunctionBase & left, const IFunctionBase & right);

/// Checks the audited native numeric-to-Decimal and Decimal-to-String conversions whose values do
/// not depend on captured conversion settings. Unsupported conversions must continue to use the
/// complete IFunctionBase::updateHash value. Callers remain responsible for comparing constant
/// argument values.
bool areNativeConversionsEquivalentForBloomFilterExpressionMatching(const IFunctionBase & left, const IFunctionBase & right);

}
