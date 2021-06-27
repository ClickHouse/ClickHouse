#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeString.h>
#include <Core/Field.h>

#if !defined(ARCADIA_BUILD)
#    include <Common/config_version.h>
#endif

namespace DB
{

/** version() - returns the current version as a string.
  */
class FunctionVersion : public IFunction
{
public:
    static constexpr auto name = "version";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionVersion>();
    }

    String getName() const override
    {
        return name;
    }

    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool isSuitableForConstantFolding() const override { return false; }

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
        return DataTypeString().createColumnConst(input_rows_count, VERSION_STRING);
    }
};


void registerFunctionVersion(FunctionFactory & factory)
{
    factory.registerFunction<FunctionVersion>(FunctionFactory::CaseInsensitive);
}

}
