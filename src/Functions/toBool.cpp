#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/CastOverloadResolver.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>


namespace DB
{
namespace
{
    class FunctionToBool : public IFunction
    {
    private:
        ContextPtr context;

        static String getReturnTypeName(const DataTypePtr & argument)
        {
            return argument->isNullable() ? "Nullable(Bool)" : "Bool";
        }

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
            return DataTypeFactory::instance().get(getReturnTypeName(arguments[0]));
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t) const override
        {
            ColumnsWithTypeAndName cast_args
            {
                arguments[0],
                {
                    DataTypeString().createColumnConst(arguments[0].column->size(), getReturnTypeName(arguments[0].type)),
                    std::make_shared<DataTypeString>(),
                    ""
                }
            };

            auto func_cast = createInternalCast(arguments[0], result_type, CastType::nonAccurate, {});
            return func_cast->execute(cast_args, result_type, arguments[0].column->size());
        }
    };
}

REGISTER_FUNCTION(ToBool)
{
    factory.registerFunction<FunctionToBool>();
}

}
