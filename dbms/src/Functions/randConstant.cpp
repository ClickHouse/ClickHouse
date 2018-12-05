#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsRandom.h>

namespace DB
{

template <typename ToType, typename Name>
class FunctionRandomConstant : public IFunction
{
private:
    /// The value is one for different blocks.
    bool is_initialized = false;
    ToType value;

public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionRandomConstant>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() > 1)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 0 or 1.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return std::make_shared<DataTypeNumber<ToType>>();
    }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        if (!is_initialized)
        {
            is_initialized = true;
            typename ColumnVector<ToType>::Container vec_to(1);
            RandImpl::execute(reinterpret_cast<char *>(vec_to.data()), sizeof(ToType));
            value = vec_to[0];
        }

        block.getByPosition(result).column = DataTypeNumber<ToType>().createColumnConst(input_rows_count, toField(value));
    }
};

struct NameRandConstant { static constexpr auto name = "randConstant"; };
using FunctionRandConstant = FunctionRandomConstant<UInt32, NameRandConstant>;

void registerFunctionRandConstant(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRandConstant>();
}

}


