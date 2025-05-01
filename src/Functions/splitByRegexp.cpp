#include <Columns/ColumnConst.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionTokens.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/Regexps.h>
#include <Common/StringUtils.h>
#include <Common/assert_cast.h>

#include <ranges>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}


/** Functions that split strings into an array of strings or vice versa.
  *
  * splitByRegexp(regexp, s[, max_substrings])
  */
namespace
{

using Pos = const char *;

class SplitByRegexpImpl
{
private:
    Regexps::RegexpPtr re;
    OptimizedRegularExpression::MatchVec matches;

    Pos pos;
    Pos end;

    std::optional<size_t> max_splits;
    size_t splits;
    bool max_substrings_includes_remaining_string;

public:
    static constexpr auto name = "splitByRegexp";

    static bool isVariadic() { return true; }
    static size_t getNumberOfArguments() { return 0; }

    static ColumnNumbers getArgumentsThatAreAlwaysConstant() { return {0, 2}; }

    static void checkArguments(const IFunction & func, const ColumnsWithTypeAndName & arguments)
    {
        checkArgumentsWithSeparatorAndOptionalMaxSubstrings(func, arguments);
    }

    static constexpr auto strings_argument_position = 1uz;

    void init(const ColumnsWithTypeAndName & arguments, bool max_substrings_includes_remaining_string_)
    {
        const ColumnConst * col = checkAndGetColumnConstStringOrFixedString(arguments[0].column.get());

        if (!col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}. "
                            "Must be constant string.", arguments[0].column->getName(), name);

        if (!col->getValue<String>().empty())
            re = std::make_shared<OptimizedRegularExpression>(Regexps::createRegexp<false, false, false>(col->getValue<String>()));

        max_substrings_includes_remaining_string = max_substrings_includes_remaining_string_;
        max_splits = extractMaxSplits(arguments, 2);
    }

    /// Called for each next string.
    void set(Pos pos_, Pos end_)
    {
        pos = pos_;
        end = end_;
        splits = 0;
    }

    /// Get the next token, if any, or return false.
    bool get(Pos & token_begin, Pos & token_end)
    {
        if (!re)
        {
            if (pos == end)
                return false;

            token_begin = pos;

            if (max_splits)
            {
                if (max_substrings_includes_remaining_string)
                {
                    if (splits == *max_splits - 1)
                    {
                        token_end = end;
                        pos = end;
                        return true;
                    }
                }
                else
                    if (splits == *max_splits)
                        return false;
            }

            ++pos;
            token_end = pos;
            ++splits;
        }
        else
        {
            if (!pos || pos > end)
                return false;

            token_begin = pos;

            if (max_splits)
            {
                if (max_substrings_includes_remaining_string)
                {
                    if (splits == *max_splits - 1)
                    {
                        token_end = end;
                        pos = nullptr;
                        return true;
                    }
                }
                else
                    if (splits == *max_splits)
                        return false;
            }

            if (!re->match(pos, end - pos, matches) || !matches[0].length)
            {
                token_end = end;
                pos = end + 1;
            }
            else
            {
                token_end = pos + matches[0].offset;
                pos = token_end + matches[0].length;
                ++splits;
            }
        }

        return true;
    }
};

using FunctionSplitByRegexp = FunctionTokens<SplitByRegexpImpl>;

/// Fallback splitByRegexp to splitByChar when its 1st argument is a trivial char for better performance
class SplitByRegexpOverloadResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = "splitByRegexp";
    static FunctionOverloadResolverPtr create(ContextPtr context) { return std::make_unique<SplitByRegexpOverloadResolver>(context); }

    explicit SplitByRegexpOverloadResolver(ContextPtr context_)
        : context(context_)
        , split_by_regexp(FunctionSplitByRegexp::create(context)) {}

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return SplitByRegexpImpl::getNumberOfArguments(); }
    bool isVariadic() const override { return SplitByRegexpImpl::isVariadic(); }
    /// ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return SplitByRegexpImpl::getArgumentsThatAreAlwaysConstant(); }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        if (patternIsTrivialChar(arguments))
            return FunctionFactory::instance().getImpl("splitByChar", context)->build(arguments);
        return std::make_unique<FunctionToFunctionBaseAdaptor>(
            split_by_regexp,
            DataTypes{std::from_range_t{}, arguments | std::views::transform([](auto & elem) { return elem.type; })},
            return_type);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        return split_by_regexp->getReturnTypeImpl(arguments);
    }

private:
    bool patternIsTrivialChar(const ColumnsWithTypeAndName & arguments) const
    {
        if (!arguments[0].column.get())
            return false;
        const ColumnConst * col = checkAndGetColumnConstStringOrFixedString(arguments[0].column.get());
        if (!col)
            return false;

        String pattern = col->getValue<String>();
        if (pattern.size() == 1)
        {
            OptimizedRegularExpression re = Regexps::createRegexp<false, false, false>(pattern);

            std::string required_substring;
            bool is_trivial;
            bool required_substring_is_prefix;
            re.getAnalyzeResult(required_substring, is_trivial, required_substring_is_prefix);
            return is_trivial && required_substring == pattern;
        }
        return false;
    }

    ContextPtr context;
    FunctionPtr split_by_regexp;
};
}

REGISTER_FUNCTION(SplitByRegexp)
{
    factory.registerFunction<SplitByRegexpOverloadResolver>();
}

}
