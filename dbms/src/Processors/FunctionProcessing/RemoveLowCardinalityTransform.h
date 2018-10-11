#pragma once

#include <Processors/ITransform.h>
#include <Core/ColumnNumbers.h>

namespace DB
{

class PreparedFunctionLowCardinalityResultCache;
using PreparedFunctionLowCardinalityResultCachePtr = std::shared_ptr<PreparedFunctionLowCardinalityResultCache>;

class RemoveLowCardinalityTransform : public ITransform
{
public:
    RemoveLowCardinalityTransform(
        Block input_header,
        const ColumnNumbers & column_numbers,
        size_t result,
        bool can_be_executed_on_default_arguments,
        PreparedFunctionLowCardinalityResultCachePtr cache);

    String getName() const override { return "RemoveLowCardinalityTransform"; }

protected:
    Blocks transform(Blocks && blocks) override;

private:
    ColumnNumbers column_numbers;
    size_t result;

    bool can_be_executed_on_default_arguments;
    PreparedFunctionLowCardinalityResultCachePtr cache;
};

}
