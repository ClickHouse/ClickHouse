#pragma once;

#include <Processors/ITransform.h>
#include <Core/ColumnNumbers.h>
#include <Functions/Helpers/PreparedFunctionLowCardinalityResultCache.h>

namespace DB
{

class RemoveLowCardinalityTransform : public ITransform
{
public:
    RemoveLowCardinalityTransform(Block input_header, const ColumnNumbers & column_numbers, size_t result,
                                  bool can_be_executed_on_default_arguments);

    void setCache(PreparedFunctionLowCardinalityResultCachePtr cache_) { cache = std::move(cache_); }

protected:
    Blocks transform(Blocks && blocks) override;

private:
    const ColumnNumbers & column_numbers;
    size_t result;

    bool can_be_executed_on_default_arguments;
    PreparedFunctionLowCardinalityResultCachePtr cache;
};

}
