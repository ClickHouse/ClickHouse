#if defined(__ELF__) && !defined(__FreeBSD__)

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeString.h>
#include <Common/SymbolIndex.h>
#include <Core/Field.h>


namespace DB
{
namespace
{

/** buildId() - returns the compiler build id of the running binary.
  */
class FunctionBuildId : public IFunction
{
public:
    static constexpr auto name = "buildId";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionBuildId>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return DataTypeString().createColumnConst(input_rows_count, SymbolIndex::instance()->getBuildIDHex());
    }
};

}

void registerFunctionBuildId(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBuildId>();
}

}

#else

namespace DB
{
class FunctionFactory;
void registerFunctionBuildId(FunctionFactory &) {}
}

#endif
