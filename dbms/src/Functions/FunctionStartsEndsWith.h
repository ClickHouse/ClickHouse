#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <Functions/GatherUtils/Sources.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>


namespace DB
{

using namespace GatherUtils;

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

struct NameStartsWith
{
    static constexpr auto name = "startsWith";
};
struct NameEndsWith
{
    static constexpr auto name = "endsWith";
};

template <typename Name>
class FunctionStartsEndsWith : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionStartsEndsWith>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isStringOrFixedString(arguments[0]))
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!isStringOrFixedString(arguments[1]))
            throw Exception("Illegal type " + arguments[1]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        const IColumn * haystack_column = block.getByPosition(arguments[0]).column.get();
        const IColumn * needle_column = block.getByPosition(arguments[1]).column.get();

        auto col_res = ColumnVector<UInt8>::create();
        typename ColumnVector<UInt8>::Container & vec_res = col_res->getData();

        vec_res.resize(input_rows_count);

        if (const ColumnString * haystack = checkAndGetColumn<ColumnString>(haystack_column))
            dispatch<StringSource>(StringSource(*haystack), needle_column, vec_res);
        else if (const ColumnFixedString * haystack = checkAndGetColumn<ColumnFixedString>(haystack_column))
            dispatch<FixedStringSource>(FixedStringSource(*haystack), needle_column, vec_res);
        else if (const ColumnConst * haystack = checkAndGetColumnConst<ColumnString>(haystack_column))
            dispatch<ConstSource<StringSource>>(ConstSource<StringSource>(*haystack), needle_column, vec_res);
        else if (const ColumnConst * haystack = checkAndGetColumnConst<ColumnFixedString>(haystack_column))
            dispatch<ConstSource<FixedStringSource>>(ConstSource<FixedStringSource>(*haystack), needle_column, vec_res);
        else
            throw Exception("Illegal combination of columns as arguments of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);

        block.getByPosition(result).column = std::move(col_res);
    }

private:
    template <typename HaystackSource>
    void dispatch(HaystackSource haystack_source, const IColumn * needle_column, PaddedPODArray<UInt8> & res_data) const
    {
        if (const ColumnString * needle = checkAndGetColumn<ColumnString>(needle_column))
            execute<HaystackSource, StringSource>(haystack_source, StringSource(*needle), res_data);
        else if (const ColumnFixedString * needle = checkAndGetColumn<ColumnFixedString>(needle_column))
            execute<HaystackSource, FixedStringSource>(haystack_source, FixedStringSource(*needle), res_data);
        else if (const ColumnConst * needle = checkAndGetColumnConst<ColumnString>(needle_column))
            execute<HaystackSource, ConstSource<StringSource>>(haystack_source, ConstSource<StringSource>(*needle), res_data);
        else if (const ColumnConst * needle = checkAndGetColumnConst<ColumnFixedString>(needle_column))
            execute<HaystackSource, ConstSource<FixedStringSource>>(haystack_source, ConstSource<FixedStringSource>(*needle), res_data);
        else
            throw Exception("Illegal combination of columns as arguments of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);
    }

    template <typename HaystackSource, typename NeedleSource>
    static void execute(HaystackSource haystack_source, NeedleSource needle_source, PaddedPODArray<UInt8> & res_data)
    {
        size_t row_num = 0;

        while (!haystack_source.isEnd())
        {
            auto haystack = haystack_source.getWhole();
            auto needle = needle_source.getWhole();

            if (needle.size > haystack.size)
            {
                res_data[row_num] = false;
            }
            else
            {
                if constexpr (std::is_same_v<Name, NameStartsWith>)
                {
                    res_data[row_num] = StringRef(haystack.data, needle.size) == StringRef(needle.data, needle.size);
                }
                else    /// endsWith
                {
                    res_data[row_num] = StringRef(haystack.data + haystack.size - needle.size, needle.size) == StringRef(needle.data, needle.size);
                }
            }

            haystack_source.next();
            needle_source.next();
            ++row_num;
        }
    }
};

}
