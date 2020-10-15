#pragma once

#include <Functions/IFunctionImpl.h>

namespace DB
{

class TupleElementWithIndexOverloadResolver : public IFunctionOverloadResolverImpl
{
public:
    String name = "TupleElementWithIndex";
    TupleElementWithIndexOverloadResolver(size_t index_) : index(index_) {}
    String getName() const override { return name + "_" + std::to_string(index); }
    size_t getNumberOfArguments() const override { return 1; }
    DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments) const override;
    FunctionBaseImplPtr build(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override;
    size_t index{};
};

}
