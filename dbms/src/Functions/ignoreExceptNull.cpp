#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

/** ignoreExceptNull(...) is a function that takes any arguments, and always returns 0 except Null.
  */
    class FunctionIgnoreExceptNull : public IFunction
    {
    public:
        static constexpr auto name = "ignoreExceptNull";
        static FunctionPtr create(const Context &)
        {
            return std::make_shared<FunctionIgnoreExceptNull>();
        }

        bool isVariadic() const override
        {
            return true;
        }
        size_t getNumberOfArguments() const override
        {
            return 0;
        }

        String getName() const override
        {
            return name;
        }

        DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
        {
            return std::make_shared<DataTypeUInt8>();
        }

        void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
        {
            block.getByPosition(result).column = DataTypeUInt8().createColumnConst(input_rows_count, UInt64(0));
        }
    };


    void registerFunctionIgnoreExceptNull(FunctionFactory & factory)
    {
        factory.registerFunction<FunctionIgnoreExceptNull>();
    }

}
