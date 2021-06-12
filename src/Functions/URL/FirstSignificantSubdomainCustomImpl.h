#pragma once

#include <Functions/FunctionFactory.h>
#include <Functions/URL/FunctionsURL.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Common/TLDListsHolder.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

struct FirstSignificantSubdomainCustomLookup
{
    const TLDList & tld_list;
    FirstSignificantSubdomainCustomLookup(const std::string & tld_list_name)
        : tld_list(TLDListsHolder::getInstance().getTldList(tld_list_name))
    {
    }

    bool operator()(const char *pos, size_t len) const
    {
        return tld_list.has(StringRef{pos, len});
    }
};

template <typename Extractor, typename Name>
class FunctionCutToFirstSignificantSubdomainCustomImpl : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionCutToFirstSignificantSubdomainCustomImpl>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!isString(arguments[0].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}. Must be String.",
                arguments[0].type->getName(), getName());
        if (!isString(arguments[1].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument (TLD_list_name) of function {}. Must be String/FixedString.",
                arguments[1].type->getName(), getName());
        const auto * column = arguments[1].column.get();
        if (!column || !checkAndGetColumnConstStringOrFixedString(column))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "The second argument of function {} should be a constant string with the name of the custom TLD",
                getName());

        return arguments[0].type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const override
    {
        const ColumnConst * column_tld_list_name = checkAndGetColumnConstStringOrFixedString(arguments[1].column.get());
        FirstSignificantSubdomainCustomLookup tld_lookup(column_tld_list_name->getValue<String>());

        /// FIXME: convertToFullColumnIfConst() is suboptimal
        auto column = arguments[0].column->convertToFullColumnIfConst();
        if (const ColumnString * col = checkAndGetColumn<ColumnString>(*column))
        {
            auto col_res = ColumnString::create();
            vector(tld_lookup, col->getChars(), col->getOffsets(), col_res->getChars(), col_res->getOffsets());
            return col_res;
        }
        else
            throw Exception(
                "Illegal column " + arguments[0].column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }

    static void vector(FirstSignificantSubdomainCustomLookup & tld_lookup,
        const ColumnString::Chars & data, const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data, ColumnString::Offsets & res_offsets)
    {
        size_t size = offsets.size();
        res_offsets.resize(size);
        res_data.reserve(size * Extractor::getReserveLengthForElement());

        size_t prev_offset = 0;
        size_t res_offset = 0;

        /// Matched part.
        Pos start;
        size_t length;

        for (size_t i = 0; i < size; ++i)
        {
            Extractor::execute(tld_lookup, reinterpret_cast<const char *>(&data[prev_offset]), offsets[i] - prev_offset - 1, start, length);

            res_data.resize(res_data.size() + length + 1);
            memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], start, length);
            res_offset += length + 1;
            res_data[res_offset - 1] = 0;

            res_offsets[i] = res_offset;
            prev_offset = offsets[i];
        }
    }
};

}
