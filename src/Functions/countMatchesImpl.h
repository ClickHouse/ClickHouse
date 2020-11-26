#pragma once

#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/Regexps.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
}

using Pos = const char *;

class FunctionCountMatches : public IFunction
{
public:
    static constexpr auto name = "countMatches";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionCountMatches>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[1]))
            throw Exception("Illegal type " + arguments[1]->getName() + " of second argument of function " + getName() + ". Must be String.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        
        return std::make_shared<DataTypeUInt64>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) const override
    {

        const ColumnConst * col = checkAndGetColumnConstStringOrFixedString(block.getByPosition(arguments[1]).column.get());

        if (!col)
            throw Exception("Illegal column " + block.getByPosition(arguments[1]).column->getName()
                + " of first argument of function " + getName() + ". Must be constant string.",
                ErrorCodes::ILLEGAL_COLUMN);

        Regexps::Pool::Pointer re = Regexps::get<false, false>(col->getValue<String>());
        size_t capture = re->getNumberOfSubpatterns() > 0 ? 1 : 0;
        OptimizedRegularExpression::MatchVec matches;
        matches.resize(capture + 1);

        size_t array_argument_position = arguments[0];

        const ColumnString * col_str = checkAndGetColumn<ColumnString>(block.getByPosition(array_argument_position).column.get());
        const ColumnConst * col_const_str =
                checkAndGetColumnConstStringOrFixedString(block.getByPosition(array_argument_position).column.get());

        auto col_res = ColumnUInt64::create();
        ColumnUInt64::Container & vec_res = col_res->getData();

        if (col_str)
        {
            const ColumnString::Chars & src_chars = col_str->getChars();
            const ColumnString::Offsets & src_offsets = col_str->getOffsets();

            vec_res.resize(src_offsets.size());

            size_t size = src_offsets.size();
            ColumnString::Offset current_src_offset = 0;

            for (size_t i = 0; i < size; ++i)
            {
                Pos pos = reinterpret_cast<Pos>(&src_chars[current_src_offset]);
                current_src_offset = src_offsets[i];
                Pos end = reinterpret_cast<Pos>(&src_chars[current_src_offset]) - 1;

                uint64_t match_count = 0;
                while (true)
                {
                    if (!pos || pos > end)
                        break;
                    if (!re->match(pos, end - pos, matches) || !matches[0].length)
                        break;
                    pos += matches[0].offset + matches[0].length;
                    match_count++;
                }
                vec_res[i] = match_count;
            }

            block.getByPosition(result).column = std::move(col_res);
        }
        else if (col_const_str)
        {
            String src = col_const_str->getValue<String>();
 
            Pos pos = reinterpret_cast<Pos>(src.data());
            Pos end = reinterpret_cast<Pos>(src.data() + src.size());

            uint64_t match_count = 0;
            while (true)
            {
                if (!pos || pos > end)
                    break;
                if (!re->match(pos, end - pos, matches) || !matches[0].length)
                    break;
                pos += matches[0].offset + matches[0].length;
                match_count++;
            }

            block.getByPosition(result).column = DataTypeUInt64().createColumnConst(col_const_str->size(), match_count);
        }
        else
            throw Exception("Illegal columns " + block.getByPosition(array_argument_position).column->getName()
                    + ", " + block.getByPosition(array_argument_position).column->getName()
                    + " of arguments of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

}
