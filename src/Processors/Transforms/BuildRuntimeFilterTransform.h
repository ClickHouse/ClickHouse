#pragma once
#include <memory>
#include <Processors/ISimpleTransform.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>

namespace DB
{

class IFunctionBase;
using FunctionBasePtr = std::shared_ptr<const IFunctionBase>;

/// Implements building a Bloom Filter from all values of the specified column. When building is finished the filter is saved into
/// per-query filter map under the specified name. This allows to find the filter by name and use it in Expressions with the help of
/// a special function 'filterContains'
class BuildRuntimeFilterTransform : public ISimpleTransform
{
public:
    BuildRuntimeFilterTransform(
        SharedHeader header_,
        String filter_column_name_,
        const DataTypePtr & filter_column_type_,
        String filter_name_,
        UInt64 exact_values_limit_,
        UInt64 bloom_filter_bytes_,
        UInt64 bloom_filter_hash_functions_);

    String getName() const override { return "BuildRuntimeFilterTransform"; }

    Status prepare() override;

    void transform(Chunk & chunk) override;

private:
    const String filter_column_name;
    const size_t filter_column_position = -1;
    const DataTypePtr filter_column_original_type;
    const DataTypePtr filter_column_target_type;
    const String filter_name;

    FunctionBasePtr cast_to_target_type;

    std::unique_ptr<RuntimeFilter> built_filter;

    void finish();
};

}
