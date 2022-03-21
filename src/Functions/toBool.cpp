#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsConversion.h>
#include <Functions/CastOverloadResolver.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>


namespace DB
{
namespace
{
    class FunctionToBool : public IFunction
    {
    public:
        static constexpr auto name = "toBool";

        static FunctionPtr create(ContextPtr)
        {
            return std::make_shared<FunctionToBool>();
        }

        std::string getName() const override
        {
            return name;
        }

        size_t getNumberOfArguments() const override { return 1; }
        bool useDefaultImplementationForConstants() const override { return true; }
        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

        DataTypePtr getReturnTypeImpl(const DataTypes &) const override
        {
            return DataTypeFactory::instance().get("Bool");
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
        {
            ColumnsWithTypeAndName cast_args
            {
                arguments[0],
                {
                    DataTypeString().createColumnConst(arguments[0].column->size(), "Bool"),
                    std::make_shared<DataTypeString>(),
                    ""
                }
            };

            FunctionOverloadResolverPtr func_builder_cast = CastInternalOverloadResolver<CastType::nonAccurate>::createImpl();
            auto func_cast = func_builder_cast->build(cast_args);
            return func_cast->execute(cast_args, DataTypeFactory::instance().get("Bool"), arguments[0].column->size());
        }
    };

}

void registerFunctionToBool(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToBool>();
}

}
