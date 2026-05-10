#pragma once

#include <Parsers/IParserBase.h>

#include <span>

namespace DB
{
class Interval
{
public:
    using Representation = int;

    Interval(const Representation min_, const Representation max_) : max(max_), min(min_) { }

    Representation Max() const { return max; }
    Representation Min() const { return min; }
    bool IsWithinBounds(const Representation value) const { return min <= value && value <= max; }

    static constexpr auto max_bound = std::numeric_limits<Representation>::max();
    static constexpr auto min_bound = std::numeric_limits<Representation>::min();

private:
    Representation max = max_bound;
    Representation min = min_bound;
};

template <class ParserKQLFunctionT>
std::unique_ptr<ParserKQLFunctionT> getParserKQLFunction(size_t max_query_size)
{
    auto parser_kql_function = std::make_unique<ParserKQLFunctionT>();
    parser_kql_function->setMaxQuerySize(max_query_size);
    return parser_kql_function;
}

class IParserKQLFunction
{
    template <class ParserKQLFunctionT>
    friend std::unique_ptr<ParserKQLFunctionT> getParserKQLFunction(size_t max_query_size);
protected:
    IParserKQLFunction() = default;
private:
    void setMaxQuerySize(size_t max_query_size_)
    {
        max_query_size = max_query_size_;
    }

public:
    enum class ArgumentState : uint8_t
    {
        Parsed,
        Raw
    };

    template <typename F>
    ALWAYS_INLINE static bool wrapConvertImpl(IParser::Pos & pos, const F & func)
    {
        IParser::Pos begin = pos;
        bool res = func();
        if (!res)
            pos = begin;
        return res;
    }

    struct IncreaseDepthTag
    {
    };

    template <typename F>
    ALWAYS_INLINE static bool wrapConvertImpl(IParser::Pos & pos, IncreaseDepthTag, const F & func)
    {
        IParser::Pos begin = pos;
        pos.increaseDepth();
        bool res = func();
        pos.decreaseDepth();
        if (!res)
            pos = begin;
        return res;
    }

    bool convert(String & out, IParser::Pos & pos);
    virtual const char * getName() const;
    virtual ~IParserKQLFunction() = default;

    static String generateUniqueIdentifier();
    String getArgument(const String & function_name, DB::IParser::Pos & pos, ArgumentState argument_state = ArgumentState::Parsed);
    std::vector<std::string> getArguments(
        const String & function_name,
        DB::IParser::Pos & pos,
        ArgumentState argument_state = ArgumentState::Parsed,
        const Interval & argument_count_interval = {0, Interval::max_bound});
    String getConvertedArgument(const String & fn_name, IParser::Pos & pos);
    String getExpression(IParser::Pos & pos);
    static String getExpressionStatic(IParser::Pos & pos);
    static String getKQLFunctionName(IParser::Pos & pos);
    std::optional<String>
    getOptionalArgument(const String & function_name, DB::IParser::Pos & pos, ArgumentState argument_state = ArgumentState::Parsed);
    String
    kqlCallToExpression(std::string_view function_name, std::initializer_list<const std::string_view> params, uint32_t max_depth, uint32_t max_backtracks);
    String kqlCallToExpression(std::string_view function_name, std::span<const std::string_view> params, uint32_t max_depth, uint32_t max_backtracks);
    static String escapeSingleQuotes(const String & input);

protected:
    virtual bool convertImpl(String & out, IParser::Pos & pos);

    bool directMapping(
        String & out, IParser::Pos & pos, std::string_view ch_fn, const Interval & argument_count_interval = {0, Interval::max_bound});
    static void validateEndOfFunction(const String & fn_name, IParser::Pos & pos);
private:
    size_t max_query_size = DBMS_DEFAULT_MAX_QUERY_SIZE;
};
}
