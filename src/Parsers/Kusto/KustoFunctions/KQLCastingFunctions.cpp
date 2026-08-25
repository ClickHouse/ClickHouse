#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
#include <Parsers/Kusto/KustoFunctions/KQLCastingFunctions.h>
#include <Parsers/Kusto/KustoFunctions/KQLFunctionFactory.h>

#include <Poco/String.h>
#include <Common/re2.h>


namespace DB
{
bool ToBool::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto param = getArgument(function_name, pos);
    out = fmt::format(
        "multiIf(toString({0}) = 'true', true, "
        "toString({0}) = 'false', false, toInt64OrNull(toString({0})) != 0)",
        param,
        generateUniqueIdentifier());
    return true;
}

bool ToDateTime::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto param = getArgument(function_name, pos);

    auto inner = fmt::format("parseDateTime64BestEffortOrNull(toString({0}),9,'UTC')", param);
    out = fmt::format("substring(replaceOne(toString({}), ' ', 'T'), 1, 27)", inner);
    return true;
}

bool ToDouble::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto param = getArgument(function_name, pos);
    out = fmt::format("toFloat64OrNull(toString({0}))", param);
    return true;
}

bool ToInt::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto param = getArgument(function_name, pos);
    /// Bind `param` once via the SQL `(<expr>) AS <alias>` pattern so the input expression
    /// is evaluated exactly once per row. Otherwise non-deterministic arguments such as
    /// `rand()` would be evaluated independently in each branch and could produce
    /// inconsistent results within a single `toint(...)` call.
    const auto unique = generateUniqueIdentifier();
    const auto p = "_kql_p_" + unique;
    const auto s = "_kql_s_" + unique;
    const auto i = "_kql_i_" + unique;
    const auto f = "_kql_f_" + unique;
    out = fmt::format(
        "multiIf(isNull(({0}) AS {1}), NULL, "
        "isNotNull(toInt32OrNull(toString({1}) AS {2}) AS {3}), {3}, "
        "isNotNull(toFloat64OrNull({2}) AS {4}) AND NOT isNaN({4}), "
        "CAST({4} AS Nullable(Int32)), NULL)",
        param, p, s, i, f);
    return true;
}

bool ToLong::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto param = getArgument(function_name, pos);
    /// See the comment in `ToInt::convertImpl`: bind the input once to keep the
    /// generated SQL deterministic for non-deterministic arguments.
    const auto unique = generateUniqueIdentifier();
    const auto p = "_kql_p_" + unique;
    const auto s = "_kql_s_" + unique;
    const auto i = "_kql_i_" + unique;
    const auto f = "_kql_f_" + unique;
    out = fmt::format(
        "multiIf(isNull(({0}) AS {1}), NULL, "
        "isNotNull(toInt64OrNull(toString({1}) AS {2}) AS {3}), {3}, "
        "startsWith({2}, '0x') OR startsWith({2}, '0X'), "
        "reinterpretAsInt64(reverse(unhex(right(leftPad(substr({2}, 3), 16, '0'), 16)))), "
        "isNotNull(toFloat64OrNull({2}) AS {4}) AND NOT isNaN({4}), "
        "CAST({4} AS Nullable(Int64)), NULL)",
        param, p, s, i, f);
    return true;
}

bool ToString::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;

    const auto param = getArgument(function_name, pos);
    out = fmt::format("ifNull(toString({0}), '')", param);
    return true;
}
bool ToTimeSpan::convertImpl(String & out, IParser::Pos & pos)
{
    const auto function_name = getKQLFunctionName(pos);
    if (function_name.empty())
        return false;
    ++pos;
    String arg;
    if (pos->type == TokenType::QuotedIdentifier)
        arg = String(pos->begin + 1, pos->end - 1);
    else if (pos->type == TokenType::StringLiteral)
        arg = String(pos->begin, pos->end);
    else
        arg = getConvertedArgument(function_name, pos);

    if (pos->type == TokenType::StringLiteral || pos->type == TokenType::QuotedIdentifier)
    {
        ++pos;
        try
        {
            auto result = kqlCallToExpression("time", {arg}, pos);
            out = fmt::format("{}", result);
        }
        catch (const Exception &)
        {
            out = "NULL";
        }
    }
    else
        out = fmt::format("{}", arg);

    return true;
}

bool ToDecimal::convertImpl(String & out, IParser::Pos & pos)
{
    const String fn_name = getKQLFunctionName(pos);
    if (fn_name.empty())
        return false;

    ++pos;
    String res;
    int scale = 0;
    int precision;

    if (pos->type == TokenType::QuotedIdentifier || pos->type == TokenType::StringLiteral)
    {
        res = String(pos->begin + 1, pos->end - 1);
        ++pos;
        precision = 34;
    }
    else
    {
        res = getConvertedArgument(fn_name, pos);
        precision = 17;
    }
    static const re2::RE2 expr("^[0-9]+e[+-]?[0-9]+");
    bool is_string = std::any_of(res.begin(), res.end(), ::isalpha) && !(re2::RE2::FullMatch(res, expr));

    if (is_string)
        out = "NULL";
    else if (re2::RE2::FullMatch(res, expr))
    {
        auto exponential_pos = res.find('e');
        if (res[exponential_pos + 1] == '+' || res[exponential_pos + 1] == '-')
            scale = std::stoi(res.substr(exponential_pos + 2, res.length()));
        else
            scale = std::stoi(res.substr(exponential_pos + 1, res.length()));

        out = fmt::format("toDecimal128({}::String,{})", res, scale);
    }
    else
    {
        if (const auto dot_pos = res.find('.'); dot_pos != String::npos)
        {
            const auto tmp = res.substr(0, dot_pos - 1);
            const auto tmp_length = static_cast<int>(std::ssize(tmp));
            scale = std::max(precision - tmp_length, 0);
        }
        if (scale < 0)
            out = "NULL";
        else
            out = fmt::format("toDecimal128({}::String,{})", res, scale);
    }

    return true;
}

}
