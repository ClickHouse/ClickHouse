#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/castTypeToEither.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/// tupleHammingDistance function: (Tuple(...), Tuple(...))-> N
/// Return the number of non-equal tuple elements
class FunctionTupleHammingDistance : public IFunction
{
private:
    ContextPtr context;

public:
    static constexpr auto name = "tupleHammingDistance";
    using ResultType = UInt8;

    explicit FunctionTupleHammingDistance(ContextPtr context_) : context(context_) {}
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionTupleHammingDistance>(context); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }

    Columns getTupleElements(const IColumn & column) const
    {
        if (const auto * const_column = typeid_cast<const ColumnConst *>(&column))
            return convertConstTupleToConstantElements(*const_column);

        if (const auto * column_tuple = typeid_cast<const ColumnTuple *>(&column))
        {
            Columns columns(column_tuple->tupleSize());
            for (size_t i = 0; i < columns.size(); ++i)
                columns[i] = column_tuple->getColumnPtr(i);
            return columns;
        }

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument of function {} should be tuples, got {}",
                        getName(), column.getName());
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto * left_tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());
        const auto * right_tuple = checkAndGetDataType<DataTypeTuple>(arguments[1].type.get());

        if (!left_tuple)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument 0 of function {} should be tuples, got {}",
                            getName(), arguments[0].type->getName());

        if (!right_tuple)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument 1 of function {} should be tuples, got {}",
                            getName(), arguments[1].type->getName());

        const auto & left_types = left_tuple->getElements();
        const auto & right_types = right_tuple->getElements();

        Columns left_elements;
        Columns right_elements;
        if (arguments[0].column)
            left_elements = getTupleElements(*arguments[0].column);
        if (arguments[1].column)
            right_elements = getTupleElements(*arguments[1].column);

        if (left_types.size() != right_types.size())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Expected tuples of the same size as arguments of function {}. Got {} and {}",
                            getName(), arguments[0].type->getName(), arguments[1].type->getName());

        size_t tuple_size = left_types.size();
        if (tuple_size == 0)
            return std::make_shared<DataTypeUInt8>();

        auto compare = FunctionFactory::instance().get("notEquals", context);
        auto plus = FunctionFactory::instance().get("plus", context);
        DataTypes types(tuple_size);
        for (size_t i = 0; i < tuple_size; ++i)
        {
            try
            {
                ColumnWithTypeAndName left{left_elements.empty() ? nullptr : left_elements[i], left_types[i], {}};
                ColumnWithTypeAndName right{right_elements.empty() ? nullptr : right_elements[i], right_types[i], {}};
                auto elem_compare = compare->build(ColumnsWithTypeAndName{left, right});
                types[i] = elem_compare->getResultType();
            }
            catch (DB::Exception & e)
            {
                e.addMessage("While executing function {} for tuple element {}", getName(), i);
                throw;
            }
        }

        auto res_type = types[0];
        for (size_t i = 1; i < tuple_size; ++i)
        {
            ColumnWithTypeAndName left{res_type, {}};
            ColumnWithTypeAndName right{types[i], {}};
            auto plus_elem = plus->build({left, right});
            res_type = plus_elem->getResultType();
        }

        return res_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * left_tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());
        const auto * right_tuple = checkAndGetDataType<DataTypeTuple>(arguments[1].type.get());
        const auto & left_types = left_tuple->getElements();
        const auto & right_types = right_tuple->getElements();
        auto left_elements = getTupleElements(*arguments[0].column);
        auto right_elements = getTupleElements(*arguments[1].column);

        size_t tuple_size = left_elements.size();
        if (tuple_size == 0)
            return DataTypeUInt8().createColumnConstWithDefaultValue(input_rows_count);

        auto compare = FunctionFactory::instance().get("notEquals", context);
        auto plus = FunctionFactory::instance().get("plus", context);
        ColumnsWithTypeAndName columns(tuple_size);
        for (size_t i = 0; i < tuple_size; ++i)
        {
            ColumnWithTypeAndName left{left_elements[i], left_types[i], {}};
            ColumnWithTypeAndName right{right_elements[i], right_types[i], {}};
            auto elem_compare = compare->build(ColumnsWithTypeAndName{left, right});
            columns[i].type = elem_compare->getResultType();
            columns[i].column = elem_compare->execute({left, right}, columns[i].type, input_rows_count);
        }

        auto res = columns[0];
        for (size_t i = 1; i < tuple_size; ++i)
        {
            auto plus_elem = plus->build({res, columns[i]});
            auto res_type = plus_elem->getResultType();
            res.column = plus_elem->execute({res, columns[i]}, res_type, input_rows_count);
            res.type = res_type;
        }

        return res.column;
    }
};

void registerFunctionTupleHammingDistance(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTupleHammingDistance>();
}
}
