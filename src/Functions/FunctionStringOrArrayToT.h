#pragma once
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


template <typename Impl, typename Name, typename ResultType, bool is_suitable_for_short_circuit_arguments_execution = true>
class FunctionStringOrArrayToT : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr) { return createImpl(); }
    static FunctionPtr createImpl()
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

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override
    {
        return is_suitable_for_short_circuit_arguments_execution;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isStringOrFixedString(arguments[0])
            && !isArray(arguments[0])
            && !isMap(arguments[0])
            && !isUUID(arguments[0])
            && !isIPv6(arguments[0])
            && !isIPv4(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[0]->getName(), getName());

        return std::make_shared<DataTypeNumber<ResultType>>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const ColumnPtr column = arguments[0].column;
        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnVector<ResultType>::create();

            typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
            vec_res.resize(col->size());
            Impl::vector(col->getChars(), col->getOffsets(), vec_res, input_rows_count);

            return col_res;
        }
        else if (const ColumnFixedString * col_fixed = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            if (Impl::is_fixed_to_constant)
            {
                ResultType res = 0;
                Impl::vectorFixedToConstant(col_fixed->getChars(), col_fixed->getN(), res, input_rows_count);

                return result_type->createColumnConst(col_fixed->size(), toField(res));
            }
            else
            {
                auto col_res = ColumnVector<ResultType>::create();

                typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
                vec_res.resize(col_fixed->size());
                Impl::vectorFixedToVector(col_fixed->getChars(), col_fixed->getN(), vec_res, input_rows_count);

                return col_res;
            }
        }
        else if (const ColumnArray * col_arr = checkAndGetColumn<ColumnArray>(column.get()))
        {
            auto col_res = ColumnVector<ResultType>::create();

            typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
            vec_res.resize(col_arr->size());
            Impl::array(col_arr->getOffsets(), vec_res, input_rows_count);

            return col_res;
        }
        else if (const ColumnMap * col_map = checkAndGetColumn<ColumnMap>(column.get()))
        {
            auto col_res = ColumnVector<ResultType>::create();
            typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
            vec_res.resize(col_map->size());
            const auto & col_nested = col_map->getNestedColumn();

            Impl::array(col_nested.getOffsets(), vec_res, input_rows_count);
            return col_res;
        }
        else if (const ColumnUUID * col_uuid = checkAndGetColumn<ColumnUUID>(column.get()))
        {
            auto col_res = ColumnVector<ResultType>::create();
            typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
            vec_res.resize(col_uuid->size());
            Impl::uuid(col_uuid->getData(), input_rows_count, vec_res, input_rows_count);
            return col_res;
        }
        else if (const ColumnIPv6 * col_ipv6 = checkAndGetColumn<ColumnIPv6>(column.get()))
        {
            auto col_res = ColumnVector<ResultType>::create();
            typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
            vec_res.resize(col_ipv6->size());
            Impl::ipv6(col_ipv6->getData(), input_rows_count, vec_res, input_rows_count);
            return col_res;
        }
        else if (const ColumnIPv4 * col_ipv4 = checkAndGetColumn<ColumnIPv4>(column.get()))
        {
            auto col_res = ColumnVector<ResultType>::create();
            typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
            vec_res.resize(col_ipv4->size());
            Impl::ipv4(col_ipv4->getData(), input_rows_count, vec_res, input_rows_count);
            return col_res;
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}",
                arguments[0].column->getName(), getName());
    }
};

}
