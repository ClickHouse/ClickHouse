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
        bool useDefaultImplementationForNulls() const override { return false; }
        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
        {
            auto bool_type = DataTypeFactory::instance().get("Bool");
            return arguments[0]->isNullable() ? makeNullable(bool_type) : bool_type;
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t) const override
        {
            ColumnsWithTypeAndName cast_args
            {
                arguments[0],
                {
                    DataTypeString().createColumnConst(arguments[0].column->size(), arguments[0].type->isNullable() ? "Nullable(Bool)" : "Bool"),
                    std::make_shared<DataTypeString>(),
                    ""
                }
            };

            FunctionOverloadResolverPtr func_builder_cast = CastInternalOverloadResolver<CastType::nonAccurate>::createImpl();
            auto func_cast = func_builder_cast->build(cast_args);
            return func_cast->execute(cast_args, result_type, arguments[0].column->size());
        }
    };

}

REGISTER_FUNCTION(ToBool)
{
    factory.registerFunction<FunctionToBool>();
}

}
