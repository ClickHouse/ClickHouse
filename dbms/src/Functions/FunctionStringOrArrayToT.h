#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnArray.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


template <typename Impl, typename Name, typename ResultType>
class FunctionStringOrArrayToT : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionStringOrArrayToT>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isStringOrFixedString(arguments[0])
            && !isArray(arguments[0]))
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeNumber<ResultType>>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const ColumnPtr column = block.getByPosition(arguments[0]).column;
        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnVector<ResultType>::create();

            typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
            vec_res.resize(col->size());
            Impl::vector(col->getChars(), col->getOffsets(), vec_res);

            block.getByPosition(result).column = std::move(col_res);
        }
        else if (const ColumnFixedString * col = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            if (Impl::is_fixed_to_constant)
            {
                ResultType res = 0;
                Impl::vector_fixed_to_constant(col->getChars(), col->getN(), res);

                block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(col->size(), toField(res));
            }
            else
            {
                auto col_res = ColumnVector<ResultType>::create();

                typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
                vec_res.resize(col->size());
                Impl::vector_fixed_to_vector(col->getChars(), col->getN(), vec_res);

                block.getByPosition(result).column = std::move(col_res);
            }
        }
        else if (const ColumnArray * col = checkAndGetColumn<ColumnArray>(column.get()))
        {
            auto col_res = ColumnVector<ResultType>::create();

            typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
            vec_res.resize(col->size());
            Impl::array(col->getOffsets(), vec_res);

            block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

}
