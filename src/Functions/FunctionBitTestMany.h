#pragma once

#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnVector.h>
#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>
#include <ext/range.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
}


template <typename Impl, typename Name>
struct FunctionBitTestMany : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionBitTestMany>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception{"Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be at least 2.", ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION};

        const auto & first_arg = arguments.front();

        if (!isInteger(first_arg))
            throw Exception{"Illegal type " + first_arg->getName() + " of first argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};


        for (const auto i : ext::range(1, arguments.size()))
        {
            const auto & pos_arg = arguments[i];

            if (!isUnsignedInteger(pos_arg))
                throw Exception{"Illegal type " + pos_arg->getName() + " of " + toString(i) + " argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const override
    {
        const auto * value_col = arguments.front().column.get();

        ColumnPtr res;
        if (!((res = execute<UInt8>(arguments, result_type, value_col))
            || (res = execute<UInt16>(arguments, result_type, value_col))
            || (res = execute<UInt32>(arguments, result_type, value_col))
            || (res = execute<UInt64>(arguments, result_type, value_col))
            || (res = execute<Int8>(arguments, result_type, value_col))
            || (res = execute<Int16>(arguments, result_type, value_col))
            || (res = execute<Int32>(arguments, result_type, value_col))
            || (res = execute<Int64>(arguments, result_type, value_col))))
            throw Exception{"Illegal column " + value_col->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN};

        return res;
    }

private:
    template <typename T>
    ColumnPtr execute(
            const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type,
            const IColumn * const value_col_untyped) const
    {
        if (const auto value_col = checkAndGetColumn<ColumnVector<T>>(value_col_untyped))
        {
            const auto size = value_col->size();
            bool is_const;
            const auto const_mask = createConstMaskIfConst<T>(arguments, is_const);
            const auto & val = value_col->getData();

            auto out_col = ColumnVector<UInt8>::create(size);
            auto & out = out_col->getData();

            if (is_const)
            {
                for (const auto i : ext::range(0, size))
                    out[i] = Impl::apply(val[i], const_mask);
            }
            else
            {
                const auto mask = createMask<T>(size, arguments);

                for (const auto i : ext::range(0, size))
                    out[i] = Impl::apply(val[i], mask[i]);
            }

            return out_col;
        }
        else if (const auto value_col_const = checkAndGetColumnConst<ColumnVector<T>>(value_col_untyped))
        {
            const auto size = value_col_const->size();
            bool is_const;
            const auto const_mask = createConstMaskIfConst<T>(arguments, is_const);
            const auto val = value_col_const->template getValue<T>();

            if (is_const)
            {
                return result_type->createColumnConst(size, toField(Impl::apply(val, const_mask)));
            }
            else
            {
                const auto mask = createMask<T>(size, arguments);
                auto out_col = ColumnVector<UInt8>::create(size);

                auto & out = out_col->getData();

                for (const auto i : ext::range(0, size))
                    out[i] = Impl::apply(val, mask[i]);

                return out_col;
            }
        }

        return nullptr;
    }

    template <typename ValueType>
    ValueType createConstMaskIfConst(const ColumnsWithTypeAndName & arguments, bool & out_is_const) const
    {
        out_is_const = true;
        ValueType mask = 0;

        for (const auto i : ext::range(1, arguments.size()))
        {
            if (auto pos_col_const = checkAndGetColumnConst<ColumnVector<ValueType>>(arguments[i].column.get()))
            {
                const auto pos = pos_col_const->getUInt(0);
                if (pos < 8 * sizeof(ValueType))
                    mask = mask | (ValueType(1) << pos);
            }
            else
            {
                out_is_const = false;
                return {};
            }
        }

        return mask;
    }

    template <typename ValueType>
    PaddedPODArray<ValueType> createMask(const size_t size, const ColumnsWithTypeAndName & arguments) const
    {
        PaddedPODArray<ValueType> mask(size, ValueType{});

        for (const auto i : ext::range(1, arguments.size()))
        {
            const auto * pos_col = arguments[i].column.get();

            if (!addToMaskImpl<UInt8>(mask, pos_col)
                && !addToMaskImpl<UInt16>(mask, pos_col)
                && !addToMaskImpl<UInt32>(mask, pos_col)
                && !addToMaskImpl<UInt64>(mask, pos_col))
                throw Exception{"Illegal column " + pos_col->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN};
        }

        return mask;
    }

    template <typename PosType, typename ValueType>
    bool NO_SANITIZE_UNDEFINED addToMaskImpl(PaddedPODArray<ValueType> & mask, const IColumn * const pos_col_untyped) const
    {
        if (const auto pos_col = checkAndGetColumn<ColumnVector<PosType>>(pos_col_untyped))
        {
            const auto & pos = pos_col->getData();

            for (const auto i : ext::range(0, mask.size()))
                if (pos[i] < 8 * sizeof(ValueType))
                    mask[i] = mask[i] | (ValueType(1) << pos[i]);

            return true;
        }
        else if (const auto pos_col_const = checkAndGetColumnConst<ColumnVector<PosType>>(pos_col_untyped))
        {
            const auto & pos = pos_col_const->template getValue<PosType>();
            const auto new_mask = pos < 8 * sizeof(ValueType) ? ValueType(1) << pos : 0;

            for (const auto i : ext::range(0, mask.size()))
                mask[i] = mask[i] | new_mask;

            return true;
        }

        return false;
    }
};

}
