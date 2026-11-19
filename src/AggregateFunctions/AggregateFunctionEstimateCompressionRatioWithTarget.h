#pragma once

#include <AggregateFunctions/IAggregateFunction_fwd.h>
#include <DataTypes/IDataType_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <base/types.h>

#include <optional>


namespace DB
{

struct Array;
struct Settings;

/// The body of `CODEC(...)`. Declarative codecs are rejected: the function compresses serialized values directly.
ASTPtr parseEstimateCompressionRatioCodec(const String & codec);

/// `estimateCompressionRatio` with a `target_type`. `settings` is null when no query creates the function.
AggregateFunctionPtr createAggregateFunctionEstimateCompressionRatioWithTarget(
    const DataTypes & arguments,
    const Array & parameters,
    DataTypePtr target_type,
    const std::optional<String> & codec,
    std::optional<UInt64> block_size_bytes,
    const Settings * settings);

}
