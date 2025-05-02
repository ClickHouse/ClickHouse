#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/Regexps.h>
#include <Interpreters/Context.h>
#include <base/StringRef.h>
#include <Common/FunctionDocumentation.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
    extern const int INDEX_OF_POSITIONAL_ARGUMENT_IS_OUT_OF_RANGE;
}

namespace
{
class FunctionRegexpExtract : public IFunction
{
public:
    static constexpr auto name = "regexpExtract";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionRegexpExtract>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2 && arguments.size() != 3)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}",
                getName(),
                arguments.size());

        FunctionArgumentDescriptors args{
            {"haystack", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"},
            {"pattern", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), isColumnConst, "const String"},
        };

        if (arguments.size() == 3)
            args.emplace_back(FunctionArgumentDescriptor{"index", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isInteger), nullptr, "Integer"});

        validateFunctionArguments(*this, arguments, args);

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const ColumnPtr column = arguments[0].column;
        const ColumnPtr column_pattern = arguments[1].column;
        const ColumnPtr column_index = arguments.size() > 2 ? arguments[2].column : nullptr;

        /// Check if the second argument is const column
        const ColumnConst * col_pattern = typeid_cast<const ColumnConst *>(column_pattern.get());
        if (!col_pattern)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Second argument of function {} must be constant string", getName());

        /// Check if the first argument is string column(const or not)
        const ColumnConst * col_const = typeid_cast<const ColumnConst *>(column.get());
        const ColumnString * col = nullptr;
        if (col_const)
            col = typeid_cast<const ColumnString *>(&col_const->getDataColumn());
        else
            col = typeid_cast<const ColumnString *>(column.get());
        if (!col)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());

        auto col_res = ColumnString::create();
        ColumnString::Chars & vec_res = col_res->getChars();
        ColumnString::Offsets & offsets_res = col_res->getOffsets();

        if (col_const)
            constantVector(col_const->getValue<String>(), col_pattern->getValue<String>(), column_index, vec_res, offsets_res);
        else if (!column_index || isColumnConst(*column_index))
        {
            const auto * col_const_index = typeid_cast<const ColumnConst *>(column_index.get());
            ssize_t index = !col_const_index ? 1 : col_const_index->getInt(0);
            vectorConstant(col->getChars(), col->getOffsets(), col_pattern->getValue<String>(), index, vec_res, offsets_res);
        }
        else
            vectorVector(col->getChars(), col->getOffsets(), col_pattern->getValue<String>(), column_index, vec_res, offsets_res);

        return col_res;
    }

private:
    static void saveMatch(
        const OptimizedRegularExpression::MatchVec & matches,
        size_t match_index,
        const ColumnString::Chars & data,
        size_t data_offset,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t & res_offset)
    {
        if (match_index < matches.size() && matches[match_index].offset != std::string::npos)
        {
            const auto & match = matches[match_index];
            res_data.resize(res_offset + match.length + 1);
            memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], &data[data_offset + match.offset], match.length);
            res_offset += match.length;
        }
        else
            res_data.resize(res_offset + 1);

        res_data[res_offset] = 0;
        ++res_offset;
        res_offsets.push_back(res_offset);
    }

    void vectorConstant(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        const std::string & pattern,
        ssize_t index,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets) const
    {
        const OptimizedRegularExpression regexp = Regexps::createRegexp<false, false, false>(pattern);
        unsigned capture = regexp.getNumberOfSubpatterns();
        if (index < 0 || index >= capture + 1)
            throw Exception(
                ErrorCodes::INDEX_OF_POSITIONAL_ARGUMENT_IS_OUT_OF_RANGE,
                "Index value {} for regexp pattern `{}` in function {} is out-of-range, should be in [0, {})",
                index,
                pattern,
                getName(),
                capture + 1);

        OptimizedRegularExpression::MatchVec matches;
        matches.reserve(index + 1);

        res_data.reserve(data.size() / 5);
        res_offsets.reserve(offsets.size());
        size_t prev_offset = 0;
        size_t res_offset = 0;
        for (size_t cur_offset : offsets)
        {
            regexp.match(
                reinterpret_cast<const char *>(&data[prev_offset]),
                cur_offset - prev_offset - 1,
                matches,
                static_cast<unsigned>(index + 1));

            saveMatch(matches, index, data, prev_offset, res_data, res_offsets, res_offset);
            prev_offset = cur_offset;
        }
    }

    void vectorVector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        const std::string & pattern,
        const ColumnPtr & column_index,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets) const
    {
        res_data.reserve(data.size() / 5);
        res_offsets.reserve(offsets.size());

        const OptimizedRegularExpression regexp = Regexps::createRegexp<false, false, false>(pattern);
        unsigned capture = regexp.getNumberOfSubpatterns();

        OptimizedRegularExpression::MatchVec matches;
        matches.reserve(capture + 1);
        size_t prev_offset = 0;
        size_t res_offset = 0;
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            size_t cur_offset = offsets[i];

            ssize_t index = column_index->getInt(i);
            if (index < 0 || index >= capture + 1)
                throw Exception(
                    ErrorCodes::INDEX_OF_POSITIONAL_ARGUMENT_IS_OUT_OF_RANGE,
                    "Index value {} for regexp pattern `{}` in function {} is out-of-range, should be in [0, {})",
                    index,
                    pattern,
                    getName(),
                    capture + 1);

            regexp.match(
                reinterpret_cast<const char *>(&data[prev_offset]),
                cur_offset - prev_offset - 1,
                matches,
                static_cast<unsigned>(index + 1));

            saveMatch(matches, index, data, prev_offset, res_data, res_offsets, res_offset);
            prev_offset = cur_offset;
        }
    }

    void constantVector(
        const std::string & str,
        const std::string & pattern,
        const ColumnPtr & column_index,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets) const
    {
        size_t rows = column_index->size();
        res_data.reserve(str.size() / 5);
        res_offsets.reserve(rows);

        /// Copy data into padded array to be able to use memcpySmallAllowReadWriteOverflow15.
        ColumnString::Chars padded_str;
        padded_str.insert(str.begin(), str.end());

        const OptimizedRegularExpression regexp = Regexps::createRegexp<false, false, false>(pattern);
        unsigned capture = regexp.getNumberOfSubpatterns();
        OptimizedRegularExpression::MatchVec matches;
        matches.reserve(capture + 1);
        regexp.match(reinterpret_cast<const char *>(padded_str.data()), padded_str.size(), matches, static_cast<unsigned>(capture + 1));

        size_t res_offset = 0;
        for (size_t i = 0; i < rows; ++i)
        {
            ssize_t index = column_index->getInt(i);
            if (index < 0 || index >= capture + 1)
                throw Exception(
                    ErrorCodes::INDEX_OF_POSITIONAL_ARGUMENT_IS_OUT_OF_RANGE,
                    "Index value {} for regexp pattern `{}` in function {} is out-of-range, should be in [0, {})",
                    index,
                    pattern,
                    getName(),
                    capture + 1);

            saveMatch(matches, index, padded_str, 0, res_data, res_offsets, res_offset);
        }
    }
};

}

REGISTER_FUNCTION(RegexpExtract)
{
    factory.registerFunction<FunctionRegexpExtract>(
        FunctionDocumentation{.description="Extracts the first string in haystack that matches the regexp pattern and corresponds to the regex group index."});

    /// For Spark compatibility.
    factory.registerAlias("REGEXP_EXTRACT", "regexpExtract", FunctionFactory::Case::Insensitive);
}

}
