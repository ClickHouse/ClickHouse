#pragma once

#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/IFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

template <typename Name>
class FunctionTypeOf : public IFunction
{
public:
    static constexpr auto name = Name::name;

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionTypeOf>();
    }

    String getName() const override
    {
        return Name::name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName &) const override
    {
        return std::make_shared<DataTypeString>();
    }

    void executeImpl(
        Block & block,
        const ColumnNumbers & arguments,
        size_t result_pos,
        size_t input_rows_count
    ) override
    {
        ColumnPtr to {
            block.getByPosition(result_pos).type->createColumnConst(
                input_rows_count,
                {
                    block.getByPosition(arguments.front()).type->getName()
                }
            )
        };

        block.getByPosition(result_pos).column = std::move(to);
    }
};

}
