#pragma once

#include <Processors/ITransform.h>
#include <Core/ColumnNumbers.h>

namespace DB
{

class PreparedFunctionLowCardinalityResultCache;

using PreparedFunctionLowCardinalityResultCachePtr = std::shared_ptr<PreparedFunctionLowCardinalityResultCache>;

class WrapLowCardinalityTransform : public ITransform
{
public:
    WrapLowCardinalityTransform(
            Blocks input_headers,
            const ColumnNumbers & column_numbers,
            size_t result,
            bool can_be_executed_on_default_arguments,
            PreparedFunctionLowCardinalityResultCachePtr cache);

    String getName() const override { return "WrapLowCardinalityTransform"; }

protected:
    Blocks transform(Blocks && blocks) override;

private:
    ColumnNumbers column_numbers;
    size_t result;

    bool can_be_executed_on_default_arguments;
    PreparedFunctionLowCardinalityResultCachePtr cache;
};

}
