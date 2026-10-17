#pragma once

namespace DB
{

class IFunctionBase;

/// Checks whether two CAST bases have equivalent semantics for Bloom-filter expression matching.
/// Unsupported conversions must continue to use the complete IFunctionBase::updateHash value.
bool areCastsEquivalentForBloomFilterExpressionMatching(const IFunctionBase & left, const IFunctionBase & right);

}
