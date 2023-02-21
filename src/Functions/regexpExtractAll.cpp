#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/Regexps.h>
#include <Interpreters/Context.h>
#include <base/StringRef.h>
#include <Common/Documentation.h>

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
    class FunctionRegexpExtractAll : public IFunction
    {
    public:
        using Pos = const char *;

        static constexpr auto name = "regexpExtractAll";
        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionRegexpExtractAll>(); }

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
                {"haystack", &isString<IDataType>, nullptr, "String"},
                {"pattern", &isString<IDataType>, isColumnConst, "const String"},
            };

            if (arguments.size() == 3)
                args.emplace_back(FunctionArgumentDescriptor{"index", &isInteger<IDataType>, nullptr, "Integer"});

            validateFunctionArgumentTypes(*this, arguments, args);

            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
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
            const ColumnString * col = nullptr;
            const ColumnConst * col_const = typeid_cast<const ColumnConst *>(column.get());
            if (col_const)
                col = typeid_cast<const ColumnString *>(&col_const->getDataColumn());
            else
                col = typeid_cast<const ColumnString *>(column.get());
            if (!col)
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());

            auto col_res = ColumnArray::create(ColumnString::create());
            ColumnString & res_strings = typeid_cast<ColumnString &>(col_res->getData());
            ColumnArray::Offsets & res_offsets = col_res->getOffsets();
            ColumnString::Chars & res_strings_chars = res_strings.getChars();
            ColumnString::Offsets & res_strings_offsets = res_strings.getOffsets();

            if (col_const)
                constantVector(
                    col_const->getValue<String>(),
                    col_pattern->getValue<String>(),
                    column_index,
                    res_offsets,
                    res_strings_chars,
                    res_strings_offsets);
            else if (!column_index || isColumnConst(*column_index))
            {
                const auto * col_const_index = typeid_cast<const ColumnConst *>(column_index.get());
                ssize_t index = !col_const_index ? 1 : col_const_index->getInt(0);
                vectorConstant(
                    col->getChars(),
                    col->getOffsets(),
                    col_pattern->getValue<String>(),
                    index,
                    res_offsets,
                    res_strings_chars,
                    res_strings_offsets);
            }
            else
                vectorVector(
                    col->getChars(),
                    col->getOffsets(),
                    col_pattern->getValue<String>(),
                    column_index,
                    res_offsets,
                    res_strings_chars,
                    res_strings_offsets);

            return col_res;
        }

    private:
        static void saveMatchs(
            Pos start,
            Pos end,
            const Regexps::Regexp & regexp,
            OptimizedRegularExpression::MatchVec & matches,
            size_t match_index,
            ColumnArray::Offsets & res_offsets,
            ColumnString::Chars & res_strings_chars,
            ColumnString::Offsets & res_strings_offsets,
            size_t & res_offset,
            size_t & res_strings_offset)
        {
            size_t i = 0;
            Pos pos = start;
            while (pos < end)
            {
                regexp.match(pos, end - pos, matches, static_cast<unsigned>(match_index + 1));
                if (match_index >= matches.size())
                    break;

                /// Append matched segment into res_strings_chars
                const auto & match = matches[match_index];
                if (match.offset != std::string::npos)
                {
                    res_strings_chars.resize(res_strings_offset + match.length + 1);
                    memcpySmallAllowReadWriteOverflow15(&res_strings_chars[res_strings_offset], pos + match.offset, match.length);
                    res_strings_offset += match.length;
                }
                else
                    res_strings_chars.resize(res_strings_offset + 1);

                /// Update offsets of Column:String
                res_strings_chars[res_strings_offset] = 0;
                ++res_strings_offset;
                res_strings_offsets.push_back(res_strings_offset);
                ++i;

                pos += matches[0].offset + matches[0].length;
            }

            /// Update offsets of Column:Array(String)
            res_offset += i;
            res_offsets.push_back(res_offset);
        }

        static void vectorConstant(
            const ColumnString::Chars & data,
            const ColumnString::Offsets & offsets,
            const std::string & pattern,
            ssize_t index,
            ColumnArray::Offsets & res_offsets,
            ColumnString::Chars & res_strings_chars,
            ColumnString::Offsets & res_strings_offsets)
        {
            const Regexps::Regexp regexp = Regexps::createRegexp<false, false, false>(pattern);
            unsigned capture = regexp.getNumberOfSubpatterns();
            if (index < 0 || index >= capture + 1)
                throw Exception(
                    ErrorCodes::INDEX_OF_POSITIONAL_ARGUMENT_IS_OUT_OF_RANGE,
                    "Index value {} is out of range, should be in [0, {})",
                    index,
                    capture + 1);

            OptimizedRegularExpression::MatchVec matches;
            matches.reserve(index + 1);

            res_offsets.reserve(offsets.size());
            res_strings_chars.reserve(data.size() / 3);
            res_strings_offsets.reserve(offsets.size() * 2);

            size_t res_offset = 0;
            size_t res_strings_offset = 0;
            size_t prev_offset = 0;
            for (size_t cur_offset : offsets)
            {
                Pos start = reinterpret_cast<const char *>(&data[prev_offset]);
                Pos end = start + (cur_offset - prev_offset - 1);
                saveMatchs(
                    start,
                    end,
                    regexp,
                    matches,
                    index,
                    res_offsets,
                    res_strings_chars,
                    res_strings_offsets,
                    res_offset,
                    res_strings_offset);

                prev_offset = cur_offset;
            }
        }

        static void vectorVector(
            const ColumnString::Chars & data,
            const ColumnString::Offsets & offsets,
            const std::string & pattern,
            const ColumnPtr & column_index,
            ColumnArray::Offsets & res_offsets,
            ColumnString::Chars & res_strings_chars,
            ColumnString::Offsets & res_strings_offsets)
        {
            const Regexps::Regexp regexp = Regexps::createRegexp<false, false, false>(pattern);
            unsigned capture = regexp.getNumberOfSubpatterns();

            OptimizedRegularExpression::MatchVec matches;
            matches.reserve(capture + 1);

            res_offsets.reserve(offsets.size());
            res_strings_chars.reserve(data.size() / 3);
            res_strings_offsets.reserve(offsets.size() * 2);

            size_t res_offset = 0;
            size_t res_strings_offset = 0;
            size_t prev_offset = 0;
            for (size_t i = 0; i < offsets.size(); ++i)
            {
                ssize_t index = column_index->getInt(i);
                if (index < 0 || index >= capture + 1)
                    throw Exception(
                        ErrorCodes::INDEX_OF_POSITIONAL_ARGUMENT_IS_OUT_OF_RANGE,
                        "Index value {} is out of range, should be in [0, {})",
                        index,
                        capture + 1);

                size_t cur_offset = offsets[i];
                Pos start = reinterpret_cast<const char *>(&data[prev_offset]);
                Pos end = start + (cur_offset - prev_offset - 1);
                saveMatchs(
                    start,
                    end,
                    regexp,
                    matches,
                    index,
                    res_offsets,
                    res_strings_chars,
                    res_strings_offsets,
                    res_offset,
                    res_strings_offset);

                prev_offset = cur_offset;
            }
        }

        static void constantVector(
            const std::string & str,
            const std::string & pattern,
            const ColumnPtr & column_index,
            ColumnArray::Offsets & res_offsets,
            ColumnString::Chars & res_strings_chars,
            ColumnString::Offsets & res_strings_offsets)
        {
            const Regexps::Regexp regexp = Regexps::createRegexp<false, false, false>(pattern);
            unsigned capture = regexp.getNumberOfSubpatterns();

            /// Copy data into padded array to be able to use memcpySmallAllowReadWriteOverflow15.
            ColumnString::Chars padded_str;
            padded_str.insert(str.begin(), str.end());

            Pos start = reinterpret_cast<Pos>(padded_str.data());
            Pos pos = start;
            Pos end = start + padded_str.size();
            Pos prev_pos = nullptr;
            std::vector<OptimizedRegularExpression::MatchVec> matches_groups;
            while (pos < end)
            {
                OptimizedRegularExpression::MatchVec matches;
                matches.reserve(capture + 1);
                regexp.match(pos, end - pos, matches, static_cast<unsigned>(capture + 1));
                if (capture + 1 > matches.size())
                    break;


                /// Make all the offsets based on start
                for (auto & match : matches)
                    if (match.offset != std::string::npos)
                        match.offset += pos - start;

                prev_pos = pos;
                pos = start + matches[0].offset + matches[0].length;
                /// Avoid dead loop caused by empty captured string
                if (pos == prev_pos)
                    ++pos;

                matches_groups.emplace_back(std::move(matches));
            }

            size_t rows = column_index->size();
            res_offsets.reserve(rows);
            res_strings_chars.reserve(rows * str.size() / 3);
            res_strings_offsets.reserve(rows * 2);

            size_t res_offset = 0;
            size_t res_strings_offset = 0;
            for (size_t row_i = 0; row_i < rows; ++row_i)
            {
                ssize_t index = column_index->getInt(row_i);
                if (index < 0 || index >= capture + 1)
                    throw Exception(
                        ErrorCodes::INDEX_OF_POSITIONAL_ARGUMENT_IS_OUT_OF_RANGE,
                        "Index value {} is out of range, should be in [0, {})",
                        index,
                        capture + 1);

                for (auto & matches : matches_groups)
                {
                    const auto & match = matches[index];

                    /// Append matched segment into res_strings_chars
                    if (match.offset != std::string::npos)
                    {
                        res_strings_chars.resize(res_strings_offset + match.length + 1);
                        memcpySmallAllowReadWriteOverflow15(&res_strings_chars[res_strings_offset], start + match.offset, match.length);
                        res_strings_offset += match.length;
                    }
                    else
                        res_strings_chars.resize(res_strings_offset + 1);

                    /// Update offsets of Column:String
                    res_strings_chars[res_strings_offset] = 0;
                    ++res_strings_offset;
                    res_strings_offsets.push_back(res_strings_offset);
                }

                /// Update offsets of Column:Array(String)
                res_offset += matches_groups.size();
                res_offsets.push_back(res_offset);
            }
        }
    };

}

REGISTER_FUNCTION(RegexpExtractAll)
{
    factory.registerFunction<FunctionRegexpExtractAll>(
        Documentation{"Extracts all the fragments of a string that matches the regexp pattern and corresponds to the regex group index."});

    /// For Spark compatibility.
    factory.registerAlias("REGEXP_EXTRACT_ALL", "regexpExtractAll", FunctionFactory::CaseInsensitive);
}

}
