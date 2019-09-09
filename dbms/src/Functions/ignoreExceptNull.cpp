#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>


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
            /// This function is mainly used in query analysis instead of "in" functions
            /// in the case when only header is needed and set for in is not calculated.
            /// Because of that function must return the same column type as "in" function, which is ColumnUInt8.
            auto res = ColumnUInt8::create(input_rows_count, 0);
            block.getByPosition(result).column = std::move(res);
        }
    };


    void registerFunctionIgnoreExceptNull(FunctionFactory & factory)
    {
        factory.registerFunction<FunctionIgnoreExceptNull>();
    }

}
