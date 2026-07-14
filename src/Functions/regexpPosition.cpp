#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Common/FunctionDocumentation.h>
#include <Common/OptimizedRegularExpression.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
    extern const int INDEX_OF_POSITIONAL_ARGUMENT_IS_OUT_OF_RANGE;
}

namespace
{
class FunctionRegexpPosition : public IFunction
{
public:
    static constexpr auto name = "regexpPosition";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionRegexpPosition>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 5}; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() < 2 || arguments.size() > 7)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, expected 2 to 7",
                getName(),
                arguments.size());

        FunctionArgumentDescriptors args{
            {"haystack", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"},
            {"pattern", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), isColumnConst, "const String"},
        };

        if (arguments.size() >= 3)
            args.emplace_back(FunctionArgumentDescriptor{"position", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isInteger), nullptr, "Integer"});
        if (arguments.size() >= 4)
            args.emplace_back(FunctionArgumentDescriptor{"occurrence", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isInteger), nullptr, "Integer"});
        if (arguments.size() >= 5)
            args.emplace_back(FunctionArgumentDescriptor{"return_option", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isInteger), nullptr, "Integer"});
        if (arguments.size() >= 6)
            args.emplace_back(FunctionArgumentDescriptor{"flags", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), isColumnConst, "const String"});
        if (arguments.size() >= 7)
            args.emplace_back(FunctionArgumentDescriptor{"subexpression", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isInteger), nullptr, "Integer"});

        validateFunctionArguments(*this, arguments, args);

        return std::make_shared<DataTypeUInt64>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr & column_haystack = arguments[0].column;

        const ColumnConst * col_pattern = checkAndGetColumnConst<ColumnString>(arguments[1].column.get());
        if (!col_pattern)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Second argument of function {} must be constant string", getName());
        const String pattern = col_pattern->getValue<String>();

        String flags;
        if (arguments.size() >= 6)
        {
            const ColumnConst * col_flags = checkAndGetColumnConst<ColumnString>(arguments[5].column.get());
            if (!col_flags)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Sixth argument of function {} must be constant string", getName());
            flags = col_flags->getValue<String>();
        }

        const String prepared_pattern = applyFlagsToPattern(pattern, flags, getName());

        const OptimizedRegularExpression regexp(prepared_pattern);
        const unsigned num_captures = regexp.getNumberOfSubpatterns();

        /// Detect a const haystack once and reuse its (data, size) across rows instead of materializing to a full column.
        const ColumnString * col_haystack_vector = nullptr;
        const char * const_row_data = nullptr;
        size_t const_row_size = 0;

        if (const auto * col_const_haystack = checkAndGetColumnConst<ColumnString>(column_haystack.get()))
        {
            const auto & inner = assert_cast<const ColumnString &>(col_const_haystack->getDataColumn());
            const_row_data = reinterpret_cast<const char *>(inner.getChars().data());
            const_row_size = inner.getOffsets()[0];
        }
        else
        {
            col_haystack_vector = checkAndGetColumn<ColumnString>(column_haystack.get());
            if (!col_haystack_vector)
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}", column_haystack->getName(), getName());
        }

        auto col_res = ColumnUInt64::create(input_rows_count);
        ColumnUInt64::Container & res_data = col_res->getData();

        const ColumnPtr col_position = arguments.size() >= 3 ? arguments[2].column : nullptr;
        const ColumnPtr col_occurrence = arguments.size() >= 4 ? arguments[3].column : nullptr;
        const ColumnPtr col_return_option = arguments.size() >= 5 ? arguments[4].column : nullptr;
        const ColumnPtr col_subexpression = arguments.size() >= 7 ? arguments[6].column : nullptr;

        OptimizedRegularExpression::MatchVec matches;
        matches.reserve(num_captures + 1);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const Int64 position = col_position ? col_position->getInt(i) : 1;
            const Int64 occurrence = col_occurrence ? col_occurrence->getInt(i) : 1;
            const Int64 return_option = col_return_option ? col_return_option->getInt(i) : 0;
            const Int64 subexpression = col_subexpression ? col_subexpression->getInt(i) : 0;

            if (position < 1)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument 'position' for function {} must be >= 1, got {}", getName(), position);
            if (occurrence < 1)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument 'occurrence' for function {} must be >= 1, got {}", getName(), occurrence);
            if (return_option != 0 && return_option != 1)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument 'return_option' for function {} must be 0 or 1, got {}", getName(), return_option);
            if (subexpression < 0 || subexpression > static_cast<Int64>(num_captures))
                throw Exception(
                    ErrorCodes::INDEX_OF_POSITIONAL_ARGUMENT_IS_OUT_OF_RANGE,
                    "Argument 'subexpression' value {} for regexp pattern `{}` in function {} is out-of-range, should be in [0, {}]",
                    subexpression,
                    pattern,
                    getName(),
                    num_captures);

            const char * row_data;
            size_t row_size;
            if (col_haystack_vector)
            {
                const auto & offsets = col_haystack_vector->getOffsets();
                const size_t row_data_offset = i == 0 ? 0 : offsets[i - 1];
                row_size = offsets[i] - row_data_offset;
                row_data = reinterpret_cast<const char *>(&col_haystack_vector->getChars()[row_data_offset]);
            }
            else
            {
                row_data = const_row_data;
                row_size = const_row_size;
            }

            const size_t start_offset = static_cast<size_t>(position) - 1;
            if (start_offset > row_size)
            {
                res_data[i] = 0;
                continue;
            }

            res_data[i] = findOccurrence(
                regexp,
                row_data,
                row_size,
                start_offset,
                static_cast<size_t>(occurrence),
                return_option == 1,
                static_cast<size_t>(subexpression),
                matches);
        }

        return col_res;
    }

private:
    static String applyFlagsToPattern(const String & pattern, const String & flags, const String & function_name)
    {
        if (flags.empty())
            return pattern;

        bool case_insensitive = false;
        bool multiline = false;
        bool dot_nl = false;
        for (char c : flags)
        {
            switch (c)
            {
                case 'i': case_insensitive = true; break;
                case 'c': case_insensitive = false; break;
                case 'm':
                    [[fallthrough]];
                case 'n': multiline = true; break;
                case 's': dot_nl = true; break;
                default:
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Unknown regex flag '{}' for function {}. Supported flags are 'i', 'c', 'm', 'n', 's'.",
                        String(1, c),
                        function_name);
            }
        }

        String inline_on;
        String inline_off;
        (case_insensitive ? inline_on : inline_off) += 'i';
        if (multiline)
            inline_on += 'm';
        if (dot_nl)
            inline_on += 's';

        String prefix = "(?";
        prefix += inline_on;
        if (!inline_off.empty())
        {
            prefix += '-';
            prefix += inline_off;
        }
        prefix += ')';
        return prefix + pattern;
    }

    static UInt64 findOccurrence(
        const OptimizedRegularExpression & regexp,
        const char * data,
        size_t size,
        size_t start_offset,
        size_t occurrence,
        bool return_after_match,
        size_t subexpression,
        OptimizedRegularExpression::MatchVec & matches)
    {
        const unsigned capture_limit = std::max<unsigned>(1, static_cast<unsigned>(subexpression) + 1);

        size_t cur_offset = start_offset;
        for (size_t n = 0; n < occurrence; ++n)
        {
            matches.clear();
            regexp.match(data + cur_offset, size - cur_offset, matches, capture_limit);
            if (matches.empty() || matches[0].offset == std::string::npos)
                return 0;

            const auto & whole = matches[0];
            if (n + 1 == occurrence)
            {
                if (subexpression >= matches.size() || matches[subexpression].offset == std::string::npos)
                    return 0;
                const auto & m = matches[subexpression];
                const size_t abs_offset = cur_offset + m.offset;
                return abs_offset + (return_after_match ? m.length : 0) + 1;
            }

            /// Step by 1 on zero-length matches to avoid an infinite loop.
            const size_t advance = whole.length == 0 ? 1 : whole.length;
            cur_offset += whole.offset + advance;
            if (cur_offset > size)
                return 0;
        }
        return 0;
    }
};

}

REGISTER_FUNCTION(RegexpPosition)
{
    FunctionDocumentation::Description description = R"(
Returns the byte position (1-based) of the `occurrence`-th match of `pattern` in `haystack`, starting the search at byte position `position`.

If `return_option` is 0 (default), the position of the first byte of the match is returned. If 1, the position of the first byte *after* the match is returned.

If `subexpression` is greater than 0, the position of the corresponding capture group is returned instead of the whole match.

Returns 0 if no match is found, or if the requested capture group did not participate in the match.

Provided for compatibility with PostgreSQL's `regexp_instr` (also exposed under that alias). Note that positions are byte-based, consistent with other ClickHouse regex functions; PostgreSQL's `regexp_instr` is character-based.
    )";
    FunctionDocumentation::Syntax syntax = "regexpPosition(haystack, pattern[, position[, occurrence[, return_option[, flags[, subexpression]]]]])";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "String to search in.", {"String"}},
        {"pattern", "Regular expression pattern.", {"const String"}},
        {"position", "Optional. 1-based byte position to start the search. Default: 1.", {"(U)Int*"}},
        {"occurrence", "Optional. Which match to return. Default: 1.", {"(U)Int*"}},
        {"return_option", "Optional. 0 returns the position of the match start, 1 returns the position right after the match. Default: 0.", {"(U)Int*"}},
        {"flags", "Optional. Regex flags. Supported: `i` (case-insensitive), `c` (case-sensitive), `m`/`n` (multiline anchors), `s` (dot matches newline). Default: empty.", {"const String"}},
        {"subexpression", "Optional. Index of capture group whose position to return. 0 means whole match. Default: 0.", {"(U)Int*"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns the byte position of the match, or 0 if not found.",
        {"UInt64"}
    };
    FunctionDocumentation::Examples examples = {
        {
            "Basic usage",
            R"(
SELECT
    regexpPosition('hello world', 'world'),
    regexpPosition('aXbXcXd', 'X', 1, 2),
    regexpPosition('aXbXcXd', 'X', 1, 2, 1),
    regexpPosition('Hello WORLD', 'world', 1, 1, 0, 'i'),
    regexpPosition('foo123bar456', '([a-z]+)([0-9]+)', 1, 2, 0, '', 2);
            )",
            R"(
┌─...─┬─...─┬─...─┬─...─┬─...─┐
│   7 │   4 │   5 │   7 │  10 │
└─────┴─────┴─────┴─────┴─────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 5};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionRegexpPosition>(documentation);

    factory.registerAlias("regexp_instr", "regexpPosition", FunctionFactory::Case::Insensitive);
    factory.registerAlias("regexpInstr", "regexpPosition", FunctionFactory::Case::Insensitive);
}

}
