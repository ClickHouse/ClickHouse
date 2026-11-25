#include <Interpreters/Q/QCompiler.h>

#include <Interpreters/Context.h>
#include <Interpreters/ReplaceQueryParameterVisitor.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Core/Settings.h>

#include <Common/Exception.h>

#include <functional>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <unordered_map>
#include <unordered_set>


namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_query_size;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_parser_backtracks;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
    extern const int TYPE_MISMATCH;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNKNOWN_IDENTIFIER;
}

namespace QCompilerInternals
{

namespace
{

struct Call
{
    QCompiler & compiler;
    const String & name;
    QValues & args;
    QEnvironment & env;
    QScope * scope;
    size_t offset;

    /// Set by a function that gives other names to the columns of its argument table. It moves their q types to the new
    /// names itself, so they are not inherited by the names: see `QCompiler::callBuiltin`.
    bool renames_columns = false;

    [[noreturn]] void fail(int code, const String & message) const
    {
        throwQError(code, offset, fmt::format("'{}': {}", name, message));
    }

    [[noreturn]] void unsupported(const String & what) const
    {
        fail(ErrorCodes::NOT_IMPLEMENTED, what + " is not supported");
    }

    void expectArgs(size_t count) const
    {
        if (args.size() != count)
            fail(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, fmt::format("expected {} arguments, got {}", count, args.size()));
    }

    QValue & arg(size_t i) { return args.at(i); }

    QValue scalarArg(size_t i) { return compiler.toScalar(args.at(i), offset); }

    const QTable & tableArg(size_t i)
    {
        if (args.at(i).kind != QValueKind::Table)
            fail(ErrorCodes::TYPE_MISMATCH, "expected a table");
        return *args.at(i).table;
    }
};

using Handler = std::function<QValue(Call &)>;

ASTPtr lit(Field value)
{
    return makeLiteral(std::move(value));
}

ASTPtr one()
{
    return lit(Field(static_cast<UInt64>(1)));
}

ASTPtr fn(const String & name, ASTs args)
{
    auto function = make_intrusive<ASTFunction>();
    function->name = name;
    function->arguments = makeExpressionList(std::move(args));
    function->children.push_back(function->arguments);
    return function;
}

ASTPtr fn(const String & name, ASTPtr a)
{
    return fn(name, ASTs{std::move(a)});
}

ASTPtr fn(const String & name, ASTPtr a, ASTPtr b)
{
    return fn(name, ASTs{std::move(a), std::move(b)});
}

ASTPtr fn(const String & name, ASTPtr a, ASTPtr b, ASTPtr c)
{
    return fn(name, ASTs{std::move(a), std::move(b), std::move(c)});
}

ASTPtr ident(const String & name)
{
    return makeIdentifier(name);
}

ASTPtr qualified(const String & table, const String & column)
{
    return make_intrusive<ASTIdentifier>(std::vector<String>{table, column});
}

/// An integer literal value of an atom, like `3` in `3 mavg x`.
std::optional<Int64> constantInt(const QValue & value)
{
    const IAST * ast = value.ast.get();
    if (!ast || value.depth != 0)
        return std::nullopt;
    bool negate = false;
    if (const auto * function = ast->as<ASTFunction>(); function && function->arguments && function->arguments->children.size() == 1)
    {
        if (function->name == "negate")
        {
            negate = true;
            ast = function->arguments->children[0].get();
        }
        else if (function->name == "toInt16" || function->name == "toInt32" || function->name == "toUInt8")
            ast = function->arguments->children[0].get();
    }
    const auto * literal = ast->as<ASTLiteral>();
    if (!literal)
        return std::nullopt;
    Int64 result = 0;
    if (literal->value.getType() == Field::Types::Int64)
        result = literal->value.safeGet<Int64>();
    else if (literal->value.getType() == Field::Types::UInt64)
        result = static_cast<Int64>(literal->value.safeGet<UInt64>());
    else
        return std::nullopt;
    return negate ? -result : result;
}

/// The string value of a literal symbol or string.
std::optional<String> constantString(const QValue & value)
{
    if (value.depth != 0 || !value.ast)
        return std::nullopt;
    const auto * literal = value.ast->as<ASTLiteral>();
    if (!literal || literal->value.getType() != Field::Types::String)
        return std::nullopt;
    return literal->value.safeGet<String>();
}

/// The symbols of a symbol atom or a symbol list literal.
std::optional<Strings> constantSymbols(const QValue & value)
{
    if (auto single = constantString(value))
        return Strings{*single};
    if (value.depth != 1 || !value.ast)
        return std::nullopt;
    const auto * array = value.ast->as<ASTFunction>();
    if (!array || array->name != "array")
        return std::nullopt;
    Strings result;
    for (const auto & item : array->arguments->children)
    {
        const auto * literal = item->as<ASTLiteral>();
        if (!literal || literal->value.getType() != Field::Types::String)
            return std::nullopt;
        result.push_back(literal->value.safeGet<String>());
    }
    return result;
}

/// An atomic function of one argument.
QValue unary(Call & call, const String & function, std::optional<QType> qtype = std::nullopt, bool keep_qtype = false, bool string_result = false)
{
    call.expectArgs(1);
    QValue result = call.compiler.atomic(call.name, {call.scalarArg(0)},
        [&](const ASTs & asts) { return fn(function, asts[0]); }, call.scope);
    if (keep_qtype)
        result.qtype = call.args[0].qtype;
    else
        result.qtype = qtype;
    result.qtype_is_exact = result.qtype && (!keep_qtype || call.args[0].qtype_is_exact);
    result.is_string = string_result || (keep_qtype && call.args[0].is_string);
    return result;
}

bool isNumberType(QType type);

/// The type of the result of arithmetic on two numbers of different types, as q promotes them: a float, else a real,
/// else a long, and the smaller integers make an int.
QType promoteNumbers(QType left, QType right)
{
    auto rank = [](QType type)
    {
        switch (type)
        {
            case QType::Float: return 4;
            case QType::Real: return 3;
            case QType::Long: return 2;
            default: return 1;
        }
    };
    switch (std::max(rank(left), rank(right)))
    {
        case 4: return QType::Float;
        case 3: return QType::Real;
        case 2: return QType::Long;
        default: return QType::Int;
    }
}

/// An atomic function of two arguments.
QValue binary(Call & call, const std::function<ASTPtr(ASTPtr, ASTPtr)> & make, std::optional<QType> qtype = std::nullopt)
{
    call.expectArgs(2);
    QValue left = call.scalarArg(0);
    QValue right = call.scalarArg(1);
    QValue result = call.compiler.atomic(call.name, {left, right},
        [&](const ASTs & asts) { return make(asts[0], asts[1]); }, call.scope);
    if (qtype)
        result.qtype = qtype;
    else if (left.qtype == right.qtype)
        result.qtype = left.qtype;
    else if (left.qtype && right.qtype && !left.is_string && !right.is_string && isNumberType(*left.qtype) && isNumberType(*right.qtype))
        /// The result is known to be a number, which the arithmetic of a temporal value and a number looks at.
        result.qtype = promoteNumbers(*left.qtype, *right.qtype);
    else
        result.qtype = std::nullopt;
    /// The type is exact when the function gives it, or when it follows from the exact types of both arguments.
    result.qtype_is_exact = result.qtype && (qtype || (left.qtype_is_exact && right.qtype_is_exact));
    return result;
}

QValue binaryFunction(Call & call, const String & function, std::optional<QType> qtype = std::nullopt)
{
    return binary(call, [&](ASTPtr a, ASTPtr b) { return fn(function, std::move(a), std::move(b)); }, qtype);
}

/// A comparison by the rules of q: the nulls are equal to each other and are less than every other value.
/// In ClickHouse a comparison with a `NULL` is a `NULL`, so `0N=0N` would not be true and `where x=0N` would select nothing.
/// `plain`: both values are known to have no nulls. Then the function of ClickHouse is used as it is,
/// which also lets a filter use the indices of the table.
ASTPtr compare(const String & function, ASTPtr left, ASTPtr right, bool plain)
{
    if (plain)
        return fn(function, std::move(left), std::move(right));
    if (function == "equals")
        return fn("isNotDistinctFrom", std::move(left), std::move(right));
    if (function == "notEquals")
        return fn("not", fn("isNotDistinctFrom", std::move(left), std::move(right)));

    /// The result when at least one of the values is a null.
    ASTPtr with_null;
    if (function == "less")
        with_null = fn("and", fn("isNull", left), fn("isNotNull", right));
    else if (function == "greater")
        with_null = fn("and", fn("isNotNull", left), fn("isNull", right));
    else if (function == "lessOrEquals")
        with_null = fn("isNull", left);
    else
        with_null = fn("isNull", right);
    return fn("ifNull", fn(function, left, right), std::move(with_null));
}

QValue comparison(Call & call, const String & function)
{
    call.expectArgs(2);
    bool plain = call.compiler.isNotNull(call.arg(0).ast) && call.compiler.isNotNull(call.arg(1).ast);
    return binary(call, [&](ASTPtr a, ASTPtr b) { return compare(function, std::move(a), std::move(b), plain); }, QType::Boolean);
}

/// An aggregate: a column folds into a scalar per group, a list into an atom, an atom stays as is.
QValue aggregate(Call & call, const std::function<ASTPtr(ASTPtr)> & over_column, const std::function<ASTPtr(ASTPtr)> & over_list,
    bool atom_is_one = false, std::optional<QType> qtype = std::nullopt)
{
    call.expectArgs(1);
    QValue value = call.scalarArg(0);
    QValue result;
    if (value.kind == QValueKind::Column)
    {
        if (value.depth > 0)
            call.unsupported("Aggregating a column of lists");
        result = QCompiler::scalar(over_column(call.compiler.columnForAggregation(value, call.scope)));
        result.aggregated = true;
        if (call.scope && !call.scope->grouped && !call.scope->pre_aggregation)
            call.unsupported("Aggregating outside a q-sql template");
    }
    else if (value.depth > 0 && !(value.is_string && value.depth == 0))
    {
        result = QCompiler::scalar(over_list(value.ast), value.depth - 1);
        result.aggregated = value.aggregated;
    }
    else if (atom_is_one)
    {
        result = QCompiler::scalar(value.is_string ? fn("length", value.ast) : one());
        result.aggregated = value.aggregated;
    }
    else
        result = value;
    result.qtype = qtype ? qtype : value.qtype;
    if (qtype)
        result.is_string = false;
    return result;
}

/// An aggregate function of ClickHouse over a column, and the same function over a list (`arrayReduce`),
/// so that both skip the nulls: the array functions like `arraySum` do not take nullable items at all.
QValue simpleAggregate(Call & call, const String & function, std::optional<QType> qtype = std::nullopt)
{
    return aggregate(call,
        [&](ASTPtr column) { return fn(function, std::move(column)); },
        [&](ASTPtr list) { return fn("arrayReduce", lit(Field(function)), std::move(list)); },
        false, qtype);
}

/// The items of a list without the nulls, with the type of the items not nullable.
ASTPtr withoutNulls(QCompiler & compiler, ASTPtr list)
{
    String item = compiler.newLambdaParameter();
    String kept = compiler.newLambdaParameter();
    return fn("arrayMap", makeLambda({kept}, fn("assumeNotNull", ident(kept))),
        fn("arrayFilter", makeLambda({item}, fn("isNotNull", ident(item))), std::move(list)));
}

/// The items of a list with zeros in place of the nulls.
ASTPtr nullsAsZeros(QCompiler & compiler, ASTPtr list)
{
    String item = compiler.newLambdaParameter();
    return fn("arrayMap", makeLambda({item}, fn("ifNull", ident(item), lit(Field(static_cast<UInt64>(0))))), std::move(list));
}

/// An aggregate function over ranges of a list, one range `(index, length)` for every item: the running and the moving
/// aggregates. `arrayReduceInRanges` evaluates them on a structure like a segment tree, in `n log n`. An aggregate of
/// a slice of the list for every item would make all the slices at once: `n^2` items for the prefixes.
ASTPtr reduceInRanges(QCompiler & compiler, const String & function, const ASTPtr & list, const std::function<ASTPtr(ASTPtr)> & range)
{
    String index = compiler.newLambdaParameter();
    return fn("arrayReduceInRanges", ASTs{lit(Field(function)),
        fn("arrayMap", makeLambda({index}, range(ident(index))), fn("arrayEnumerate", list)), list});
}

/// The running products of a list: each of them is the previous one times the item, the order in which q multiplies,
/// with a null taken for one. ClickHouse has no cumulative product, and the product of the prefix of every item makes
/// all the prefixes at once: `n^2` items. So the list is cut into blocks. The products at the ends of the blocks are
/// folded over the blocks, and the products inside of a block are folded from the product before the block. A fold
/// holds an array of the size of a block or of the number of the blocks, so nothing of the size `n^2` is made.
ASTPtr runningProducts(QCompiler & compiler, ASTPtr list)
{
    const UInt64 block_size = 256;
    auto one_float = [] { return fn("toFloat64", one()); };
    auto last = [](const String & array) { return fn("arrayElement", ident(array), lit(Field(static_cast<Int64>(-1)))); };
    auto no_products = [] { return fn("emptyArrayFloat64", ASTs{}); };

    /// The list with ones in place of the nulls, and its blocks.
    String items = compiler.newLambdaParameter();
    String blocks = compiler.newLambdaParameter();

    /// The product at the end of every block: the product at the end of the previous one times the items of the block.
    String ends = compiler.newLambdaParameter();
    String block_of_end = compiler.newLambdaParameter();
    String product = compiler.newLambdaParameter();
    String factor = compiler.newLambdaParameter();
    ASTPtr end_of_block = fn("arrayFold", makeLambda({product, factor}, fn("multiply", ident(product), ident(factor))),
        ident(block_of_end), fn("if", fn("empty", ident(ends)), one_float(), last(ends)));
    ASTPtr block_ends = fn("arrayFold", makeLambda({ends, block_of_end}, fn("arrayPushBack", ident(ends), end_of_block)),
        ident(blocks), no_products());

    /// The product before every block: one, and then the ends of the previous blocks.
    ASTPtr block_starts = fn("arraySlice", fn("arrayPushFront", block_ends, one_float()), one(), fn("length", ident(blocks)));

    /// The products inside of every block.
    String block = compiler.newLambdaParameter();
    String start = compiler.newLambdaParameter();
    String products = compiler.newLambdaParameter();
    String item = compiler.newLambdaParameter();
    ASTPtr products_of_block = fn("arrayFold",
        makeLambda({products, item},
            fn("arrayPushBack", ident(products), fn("multiply", fn("if", fn("empty", ident(products)), ident(start), last(products)), ident(item)))),
        ident(block), no_products());
    ASTPtr all_products = fn("arrayFlatten", fn("arrayMap", makeLambda({block, start}, products_of_block), ident(blocks), block_starts));

    /// `arrayMap(name -> body, [value])[1]` gives a name to a value that the body uses several times.
    auto bind = [](const String & name, ASTPtr value, ASTPtr body)
    {
        return fn("arrayElement", fn("arrayMap", makeLambda({name}, std::move(body)), fn("array", std::move(value))), one());
    };

    /// A block starts at the items 1, 1 + block_size, 1 + 2 * block_size, and so on.
    String value = compiler.newLambdaParameter();
    String position = compiler.newLambdaParameter();
    ASTPtr split = fn("arraySplit",
        makeLambda({value, position}, fn("equals", fn("modulo", ident(position), lit(Field(block_size))), one())),
        ident(items), fn("arrayEnumerate", ident(items)));

    String nullable_item = compiler.newLambdaParameter();
    ASTPtr without_nulls = fn("arrayMap", makeLambda({nullable_item}, fn("ifNull", ident(nullable_item), one())), std::move(list));
    return bind(items, without_nulls, bind(blocks, split, all_products));
}

/// A uniform function: a column becomes a window function, a list an array function.
QValue uniform(Call & call, const std::function<ASTPtr(ASTPtr)> & over_column, const std::function<ASTPtr(ASTPtr)> & over_list)
{
    QValue value = call.scalarArg(call.args.size() - 1);
    if (value.kind == QValueKind::Column)
    {
        if (value.depth > 0)
            call.unsupported("A uniform function of a column of lists");
        QValue result = value;
        result.ast = over_column(value.ast);
        result.windowed = true;
        return result;
    }
    if (value.depth == 0)
        return value;
    QValue result = value;
    result.ast = over_list(value.ast);
    return result;
}

/// `arrayMap((v, i) -> body, list, arrayEnumerate(list))`: a function of the items and their 1-based indices.
ASTPtr mapWithIndex(QCompiler & compiler, const ASTPtr & list, const std::function<ASTPtr(ASTPtr, ASTPtr)> & body)
{
    String item = compiler.newLambdaParameter();
    String index = compiler.newLambdaParameter();
    return fn("arrayMap", makeLambda({item, index}, body(ident(item), ident(index))), list, fn("arrayEnumerate", list));
}

ASTPtr previousItem(const ASTPtr & list, const ASTPtr & index)
{
    return fn("if", fn("equals", index, one()), lit(Field()), fn("arrayElement", list, fn("minus", index, one())));
}

/// Converts a q `like` pattern into a regular expression.
String likePatternToRegexp(const String & pattern)
{
    String result = "^";
    for (size_t i = 0; i < pattern.size(); ++i)
    {
        char c = pattern[i];
        switch (c)
        {
            case '*': result += ".*"; break;
            case '?': result += "."; break;
            case '[':
            {
                size_t close = pattern.find(']', i + 1);
                if (close == String::npos)
                {
                    result += "\\[";
                    break;
                }
                String set = pattern.substr(i + 1, close - i - 1);
                if (!set.empty() && set[0] == '^')
                    result += "[^" + set.substr(1) + "]";
                else
                    result += "[" + set + "]";
                i = close;
                break;
            }
            case '.': case '+': case '(': case ')': case '{': case '}': case '|': case '\\': case '$': case '^':
                result += '\\';
                result += c;
                break;
            default:
                result += c;
        }
    }
    result += "$";
    return result;
}

/// The ClickHouse type for a q cast name: a type name (`` `int ``) or a type char (`"i"`).
std::optional<String> castTypeName(const String & name)
{
    static const std::unordered_map<String, String> names = {
        {"boolean", "b"}, {"guid", "g"}, {"byte", "x"}, {"short", "h"}, {"int", "i"}, {"long", "j"}, {"real", "e"},
        {"float", "f"}, {"char", "c"}, {"symbol", "s"}, {"timestamp", "p"}, {"month", "m"}, {"date", "d"},
        {"datetime", "z"}, {"timespan", "n"}, {"minute", "u"}, {"second", "v"}, {"time", "t"},
    };
    String code = name;
    if (auto it = names.find(name); it != names.end())
        code = it->second;
    if (code.size() != 1)
        return std::nullopt;
    switch (code[0])
    {
        case 'b': return "Bool";
        case 'g': return "UUID";
        case 'x': return "UInt8";
        case 'h': return "Int16";
        case 'i': return "Int32";
        case 'j': return "Int64";
        case 'e': return "Float32";
        case 'f': return "Float64";
        case 'c': case 's': return "String";
        case 'p': return "DateTime64(9)";
        case 'm': case 'd': return "Date32";
        case 'z': return "DateTime64(3)";
        case 'n': return "Time64(9)";
        case 'u': case 'v': return "Time";
        case 't': return "Time64(3)";
        default: return std::nullopt;
    }
}

/// The q type of a cast name.
std::optional<QType> castQType(const String & name)
{
    static const std::unordered_map<String, QType> types = {
        {"b", QType::Boolean}, {"x", QType::Byte}, {"h", QType::Short}, {"i", QType::Int}, {"j", QType::Long}, {"e", QType::Real},
        {"f", QType::Float}, {"s", QType::Symbol}, {"p", QType::Timestamp}, {"m", QType::Month}, {"d", QType::Date},
        {"z", QType::Datetime}, {"n", QType::Timespan}, {"u", QType::Minute}, {"v", QType::Second}, {"t", QType::Time},
        {"boolean", QType::Boolean}, {"byte", QType::Byte}, {"short", QType::Short}, {"int", QType::Int}, {"long", QType::Long},
        {"real", QType::Real}, {"float", QType::Float}, {"symbol", QType::Symbol}, {"timestamp", QType::Timestamp},
        {"month", QType::Month}, {"date", QType::Date}, {"datetime", QType::Datetime}, {"timespan", QType::Timespan},
        {"minute", QType::Minute}, {"second", QType::Second}, {"time", QType::Time},
    };
    if (auto it = types.find(name); it != types.end())
        return it->second;
    return std::nullopt;
}

/// A temporal accessor: `x.hh`, `` `hh$x ``.
std::optional<std::pair<std::function<ASTPtr(ASTPtr)>, QType>> accessor(const String & name)
{
    using Make = std::function<ASTPtr(ASTPtr)>;
    static const std::unordered_map<String, std::pair<Make, QType>> accessors = {
        {"year", {[](ASTPtr x) { return fn("toYear", x); }, QType::Int}},
        {"mm", {[](ASTPtr x) { return fn("toMonth", x); }, QType::Int}},
        {"dd", {[](ASTPtr x) { return fn("toDayOfMonth", x); }, QType::Int}},
        {"hh", {[](ASTPtr x) { return fn("toHour", x); }, QType::Int}},
        {"uu", {[](ASTPtr x) { return fn("toMinute", x); }, QType::Int}},
        {"ss", {[](ASTPtr x) { return fn("toSecond", x); }, QType::Int}},
        {"date", {[](ASTPtr x) { return fn("toDate32", x); }, QType::Date}},
        {"month", {[](ASTPtr x) { return fn("toStartOfMonth", fn("toDate32", x)); }, QType::Month}},
        {"week", {[](ASTPtr x) { return fn("toMonday", fn("toDate32", x)); }, QType::Date}},
        {"minute", {[](ASTPtr x) { return fn("toTime", fn("toStartOfMinute", x)); }, QType::Minute}},
        {"second", {[](ASTPtr x) { return fn("toTime", x); }, QType::Second}},
        {"time", {[](ASTPtr x) { return fn("toTime64", x, lit(Field(static_cast<UInt64>(3)))); }, QType::Time}},
    };
    if (auto it = accessors.find(name); it != accessors.end())
        return it->second;
    return std::nullopt;
}

bool isIntegerType(QType type)
{
    return type == QType::Byte || type == QType::Short || type == QType::Int || type == QType::Long;
}

bool isNumberType(QType type)
{
    return isIntegerType(type) || type == QType::Boolean || type == QType::Real || type == QType::Float;
}

bool isTemporalType(QType type)
{
    switch (type)
    {
        case QType::Timestamp:
        case QType::Month:
        case QType::Date:
        case QType::Datetime:
        case QType::Timespan:
        case QType::Minute:
        case QType::Second:
        case QType::Time:
            return true;
        default:
            return false;
    }
}

/// The origin of the temporal types of q: 2000.01.01. It is written as a literal, like the temporal literals of q,
/// so both are read in the time zone of the session.
ASTPtr epochDateTime(UInt64 scale)
{
    return fn("toDateTime64", lit(Field(String("2000-01-01 00:00:00"))), lit(Field(scale)));
}

ASTPtr epochDate()
{
    return fn("toDate32", lit(Field(String("2000-01-01"))));
}

/// q rounds a number when it converts it to an integer or to a count of units.
ASTPtr roundedInteger(ASTPtr number)
{
    return fn("toInt64", fn("round", std::move(number)));
}

/// A temporal value from a number: the number counts the units of the type in q, from its origin in q.
/// A timestamp is nanoseconds since 2000.01.01, a datetime is days (with a fraction), a date is days, a month is months,
/// a timespan is nanoseconds, a time is milliseconds, and a second and a minute are seconds and minutes.
ASTPtr numberToTemporal(ASTPtr number, QType type, const Call & call)
{
    auto scale = [] { return lit(Field(static_cast<UInt64>(9))); };
    switch (type)
    {
        case QType::Timestamp:
            return fn("addNanoseconds", epochDateTime(9), roundedInteger(std::move(number)));
        case QType::Datetime:
            return fn("addMilliseconds", epochDateTime(3),
                roundedInteger(fn("multiply", std::move(number), lit(Field(static_cast<UInt64>(86'400'000))))));
        case QType::Date:
            return fn("addDays", epochDate(), roundedInteger(std::move(number)));
        case QType::Month:
            return fn("addMonths", epochDate(), roundedInteger(std::move(number)));
        case QType::Timespan:
            return fn("CAST", fn("divide", fn("toDecimal128", fn("round", std::move(number)), scale()), lit(Field(static_cast<UInt64>(1'000'000'000)))),
                lit(Field(String("Time64(9)"))));
        case QType::Time:
            return fn("CAST", fn("divide", fn("toDecimal128", fn("round", std::move(number)), scale()), lit(Field(static_cast<UInt64>(1000)))),
                lit(Field(String("Time64(3)"))));
        case QType::Second:
            return fn("CAST", roundedInteger(std::move(number)), lit(Field(String("Time"))));
        case QType::Minute:
            return fn("CAST", fn("multiply", roundedInteger(std::move(number)), lit(Field(static_cast<UInt64>(60)))), lit(Field(String("Time"))));
        default:
            call.unsupported("This cast to a temporal type");
    }
}

/// The count of the units of a temporal value in q, from its origin in q: the reverse of `numberToTemporal`.
/// It is an integer, except for a datetime, which is a count of days with a fraction.
ASTPtr temporalToNumber(ASTPtr value, QType type, const Call & call)
{
    auto scale = [] { return lit(Field(static_cast<UInt64>(9))); };
    switch (type)
    {
        case QType::Timestamp:
            return fn("minus", fn("toUnixTimestamp64Nano", fn("toDateTime64", std::move(value), scale())),
                fn("toUnixTimestamp64Nano", epochDateTime(9)));
        case QType::Datetime:
            return fn("divide",
                fn("minus", fn("toUnixTimestamp64Milli", fn("toDateTime64", std::move(value), lit(Field(static_cast<UInt64>(3))))),
                    fn("toUnixTimestamp64Milli", epochDateTime(3))),
                lit(Field(static_cast<UInt64>(86'400'000))));
        case QType::Date:
            return fn("minus", fn("toInt64", fn("toDate32", std::move(value))), fn("toInt64", epochDate()));
        case QType::Month:
            return fn("dateDiff", lit(Field(String("month"))), epochDate(), fn("toDate32", std::move(value)));
        case QType::Timespan:
            return fn("toInt64", fn("multiply", fn("toDecimal128", std::move(value), scale()), lit(Field(static_cast<UInt64>(1'000'000'000)))));
        case QType::Time:
            return fn("toInt64", fn("multiply", fn("toDecimal128", std::move(value), scale()), lit(Field(static_cast<UInt64>(1000)))));
        case QType::Second:
            return fn("toInt64", std::move(value));
        case QType::Minute:
            return fn("intDiv", fn("toInt64", std::move(value)), lit(Field(static_cast<UInt64>(60))));
        default:
            call.unsupported("This cast from a temporal type");
    }
}

QValue castValue(Call & call, const String & type_name, QValue value)
{
    /// The name of a temporal type (`` `date$x ``) is an accessor of a temporal value (the date of a timestamp), and it is
    /// the same cast as with the letter of the type (`"d"$x`) for a number, which counts the units of the type.
    bool number_to_type = !value.is_string && (!value.qtype || isNumberType(*value.qtype)) && castTypeName(type_name);
    if (auto access = accessor(type_name); access && !number_to_type)
    {
        QValue result = call.compiler.atomic(call.name, {value}, [&](const ASTs & asts) { return access->first(asts[0]); }, call.scope);
        result.qtype = access->second;
        return result;
    }

    /// `` `$"abc" ``: a symbol from a string.
    if (type_name.empty() || type_name == "s" || type_name == "symbol" || type_name == "S" || type_name == "*")
    {
        QValue result = call.compiler.atomic(call.name, {value}, [](const ASTs & asts) { return fn("toString", asts[0]); }, call.scope);
        result.qtype = QType::Symbol;
        return result;
    }

    /// Parsing from strings: `"J"$"42"`, `"D"$"2024.01.15"`.
    if (type_name.size() == 1 && type_name[0] >= 'A' && type_name[0] <= 'Z')
    {
        char code = static_cast<char>(type_name[0] - 'A' + 'a');
        std::function<ASTPtr(ASTPtr)> parse;
        auto q_date = [](ASTPtr x)
        {
            return fn("replaceRegexpOne", std::move(x), lit(Field(String(R"(^(\d{4})\.(\d{2})\.(\d{2})[DT]?)"))), lit(Field(String(R"(\1-\2-\3 )"))));
        };
        switch (code)
        {
            case 'b': parse = [](ASTPtr x) { return fn("in", x, fn("tuple", ASTs{lit(Field(String("1"))), lit(Field(String("t"))), lit(Field(String("T")))})); }; break;
            case 'h': parse = [](ASTPtr x) { return fn("toInt16OrNull", x); }; break;
            case 'i': parse = [](ASTPtr x) { return fn("toInt32OrNull", x); }; break;
            case 'j': parse = [](ASTPtr x) { return fn("toInt64OrNull", x); }; break;
            case 'e': parse = [](ASTPtr x) { return fn("toFloat32OrNull", x); }; break;
            case 'f': parse = [](ASTPtr x) { return fn("toFloat64OrNull", x); }; break;
            case 'd': parse = [=](ASTPtr x) { return fn("toDate32OrNull", fn("trimBoth", q_date(x))); }; break;
            case 'p': parse = [=](ASTPtr x) { return fn("toDateTime64OrNull", fn("trimBoth", q_date(x)), lit(Field(static_cast<UInt64>(9)))); }; break;
            case 'z': parse = [=](ASTPtr x) { return fn("toDateTime64OrNull", fn("trimBoth", q_date(x)), lit(Field(static_cast<UInt64>(3)))); }; break;
            case 'n': parse = [](ASTPtr x) { return fn("toTime64OrNull", fn("replaceRegexpOne", x, lit(Field(String("^0D"))), lit(Field(String("")))), lit(Field(static_cast<UInt64>(9)))); }; break;
            case 't': parse = [](ASTPtr x) { return fn("toTime64OrNull", x, lit(Field(static_cast<UInt64>(3)))); }; break;
            case 'u': case 'v': parse = [](ASTPtr x) { return fn("toTimeOrNull", x); }; break;
            case 'g': parse = [](ASTPtr x) { return fn("toUUIDOrNull", x); }; break;
            default: call.unsupported(fmt::format("Parsing strings as '{}'", type_name));
        }
        QValue result = call.compiler.atomic(call.name, {value}, [&](const ASTs & asts) { return parse(asts[0]); }, call.scope);
        result.is_string = false;
        result.qtype = castQType(String(1, code));
        return result;
    }

    auto type = castTypeName(type_name);
    if (!type)
        call.fail(ErrorCodes::BAD_ARGUMENTS, fmt::format("unknown type '{}'", type_name));
    if (*type == "String")
    {
        QValue result = call.compiler.atomic(call.name, {value}, [](const ASTs & asts) { return fn("toString", asts[0]); }, call.scope);
        result.qtype = QType::Symbol;
        return result;
    }
    String target = *type;
    std::optional<QType> target_type = castQType(type_name);
    std::optional<QType> source_type = value.qtype;

    /// A conversion in q is not a `CAST` of ClickHouse: a number is rounded when it becomes an integer (`CAST` truncates),
    /// and a number that becomes a temporal value, or the other way round, counts the units of the type from 2000.01.01
    /// (`CAST` takes the seconds since 1970). The values without a known type are the results of arithmetic: numbers.
    bool from_number = !value.is_string && (!source_type || isNumberType(*source_type));
    bool from_temporal = source_type && isTemporalType(*source_type);
    bool to_integer = target_type && isIntegerType(*target_type);
    bool to_number = target_type && isNumberType(*target_type) && target_type != QType::Boolean;
    bool to_temporal = target_type && isTemporalType(*target_type);

    std::function<ASTPtr(ASTPtr)> convert;
    if (from_number && to_integer)
        convert = [&](ASTPtr x) { return fn("CAST", fn("round", std::move(x)), lit(Field(target))); };
    else if (from_number && to_temporal)
        convert = [&](ASTPtr x) { return numberToTemporal(std::move(x), *target_type, call); };
    else if (from_temporal && to_number)
        convert = [&](ASTPtr x)
        {
            ASTPtr count = temporalToNumber(std::move(x), *source_type, call);
            if (to_integer)
                count = fn("round", std::move(count));
            return fn("CAST", std::move(count), lit(Field(target)));
        };
    else
        convert = [&](ASTPtr x) { return fn("CAST", std::move(x), lit(Field(target))); };

    QValue result = call.compiler.atomic(call.name, {value}, [&](const ASTs & asts) { return convert(asts[0]); }, call.scope);
    result.is_string = false;
    result.qtype = target_type;
    return result;
}

QValue tableValue(QCompiler & compiler, ASTPtr query, Names keys = {})
{
    return compiler.makeTable(std::move(query), std::move(keys));
}

/// A select of all the columns of a table, with the given changes.
QSelectBuilder selectColumns(const QTable & table)
{
    QSelectBuilder builder;
    builder.from = table.table_expression;
    for (const auto & column : table.columns)
        builder.select.push_back(ident(column.name));
    return builder;
}

bool hasColumn(const QTable & table, const String & name)
{
    for (const auto & column : table.columns)
        if (column.name == name)
            return true;
    return false;
}

/// The first `count` rows of a table (or the last ones, if `count` is negative).
QValue takeRows(Call & call, const QValue & count, const QTable & table)
{
    auto n = constantInt(count);
    if (n && *n < 0)
    {
        QSelectBuilder numbered;
        numbered.from = table.table_expression;
        numbered.select.push_back(make_intrusive<ASTAsterisk>());
        numbered.select.push_back(withAlias(fn("rowNumberInAllBlocks", ASTs{}), "__qrow"));
        QSelectBuilder counted;
        counted.from = makeSubqueryTableExpression(numbered.build());
        counted.select.push_back(make_intrusive<ASTAsterisk>());
        auto total = make_intrusive<ASTFunction>();
        total->name = "count";
        total->arguments = makeExpressionList({});
        total->children.push_back(total->arguments);
        call.compiler.windowize(total, {}, nullptr);
        counted.select.push_back(withAlias(call.compiler.windowize(total, {}, nullptr), "__qcount"));
        counted.order_by.emplace_back(ident("__qrow"), false);
        QSelectBuilder result;
        result.from = makeSubqueryTableExpression(counted.build());
        for (const auto & column : table.columns)
            result.select.push_back(ident(column.name));
        result.where = fn("greaterOrEquals", ident("__qrow"), fn("minus", ident("__qcount"), lit(Field(-*n))));
        result.order_by.emplace_back(ident("__qrow"), false);
        return tableValue(call.compiler, result.build(), table.keys);
    }

    const QValue & limit = count;
    if (limit.depth == 1)
    {
        /// `select[10 20]` or `10 20#t`: the offset and the count.
        QSelectBuilder builder = selectColumns(table);
        builder.offset = fn("arrayElement", limit.ast, one());
        builder.limit = fn("arrayElement", limit.ast, lit(Field(static_cast<UInt64>(2))));
        return tableValue(call.compiler, builder.build(), table.keys);
    }
    QSelectBuilder builder = selectColumns(table);
    builder.limit = limit.ast;
    return tableValue(call.compiler, builder.build(), table.keys);
}

QValue sortTable(Call & call, const Strings & columns, const QTable & table, bool descending)
{
    /// The rows that are equal in the columns keep their order, as in q: the number of the row is the last key of
    /// the sorting. (And being a key, it is calculated before the sorting.)
    QSelectBuilder numbered;
    numbered.from = table.table_expression;
    numbered.select.push_back(make_intrusive<ASTAsterisk>());
    numbered.select.push_back(withAlias(fn("rowNumberInAllBlocks", ASTs{}), "__qrow"));

    QSelectBuilder builder;
    builder.from = makeSubqueryTableExpression(numbered.build());
    for (const auto & column : table.columns)
        builder.select.push_back(ident(column.name));
    for (const auto & column : columns)
    {
        if (!hasColumn(table, column))
            call.fail(ErrorCodes::UNKNOWN_IDENTIFIER, fmt::format("unknown column '{}'", column));
        builder.order_by.emplace_back(ident(column), descending);
    }
    builder.order_by.emplace_back(ident("__qrow"), false);
    return tableValue(call.compiler, builder.build(), table.keys);
}

ASTPtr unionAll(const ASTs & selects)
{
    auto result = make_intrusive<ASTSelectWithUnionQuery>();
    ASTs list;
    for (const auto & select : selects)
    {
        const auto & union_query = select->as<ASTSelectWithUnionQuery &>();
        for (const auto & child : union_query.list_of_selects->children)
            list.push_back(child);
    }
    result->list_of_selects = makeExpressionList(std::move(list));
    result->children.push_back(result->list_of_selects);
    result->union_mode = SelectUnionMode::UNION_ALL;
    for (size_t i = 1; i < result->list_of_selects->children.size(); ++i)
        result->list_of_modes.push_back(SelectUnionMode::UNION_ALL);
    return result;
}

/// `t1,t2`: the rows of both tables.
QValue appendTables(Call & call, const QTable & left, const QTable & right)
{
    QSelectBuilder first = selectColumns(left);
    QSelectBuilder second;
    second.from = right.table_expression;
    for (const auto & column : left.columns)
    {
        if (!hasColumn(right, column.name))
            call.fail(ErrorCodes::TYPE_MISMATCH, fmt::format("the tables have different columns ('{}')", column.name));
        second.select.push_back(ident(column.name));
    }
    if (left.columns.size() != right.columns.size())
        call.fail(ErrorCodes::TYPE_MISMATCH, "the tables have different columns");
    return tableValue(call.compiler, unionAll({first.build(), second.build()}), left.keys);
}

/// `t1 uj t2`: the rows of both tables, with the union of the columns.
QValue unionJoin(Call & call, const QTable & left, const QTable & right)
{
    Names names;
    for (const auto & column : left.columns)
        names.push_back(column.name);
    for (const auto & column : right.columns)
        if (!hasColumn(left, column.name))
            names.push_back(column.name);

    QSelectBuilder first;
    first.from = left.table_expression;
    QSelectBuilder second;
    second.from = right.table_expression;
    for (const auto & name : names)
    {
        first.select.push_back(hasColumn(left, name) ? ident(name) : withAlias(lit(Field()), name));
        second.select.push_back(hasColumn(right, name) ? ident(name) : withAlias(lit(Field()), name));
    }
    QSelectBuilder outer;
    outer.from = makeSubqueryTableExpression(unionAll({first.build(), second.build()}));
    for (const auto & name : names)
        outer.select.push_back(ident(name));
    return tableValue(call.compiler, outer.build(), left.keys);
}

enum class QJoinKind : uint8_t
{
    Left,
    Inner,
    Equi,
    AsOf,
};

/// Joins two tables on the key columns. The columns of the right table that are also in the left one replace them where the rows match.
/// `asof_key_from_right`: the last key of an as-of join is taken from the matched row of the right table as well (`aj0`).
QValue joinTables(Call & call, QJoinKind kind, const QTable & left, const QTable & right, const Names & keys, bool asof_key_from_right = false)
{
    if (keys.empty())
        call.fail(ErrorCodes::BAD_ARGUMENTS, "the right table must be a keyed table");
    for (const auto & key : keys)
    {
        if (!hasColumn(left, key) || !hasColumn(right, key))
            call.fail(ErrorCodes::UNKNOWN_IDENTIFIER, fmt::format("the key column '{}' is not in both tables", key));
    }

    const String left_alias = "__ql";
    const String right_alias = "__qr";
    const String match = "__qmatch";

    QSelectBuilder right_marked = selectColumns(right);
    right_marked.select.push_back(withAlias(lit(Field(static_cast<UInt64>(1))), match));

    QSelectBuilder builder;
    builder.from = makeSubqueryTableExpression(selectColumns(left).build(), left_alias);

    auto table_join = make_intrusive<ASTTableJoin>();
    switch (kind)
    {
        case QJoinKind::Left:
            table_join->kind = JoinKind::Left;
            table_join->strictness = JoinStrictness::Any;
            break;
        case QJoinKind::Inner:
            table_join->kind = JoinKind::Inner;
            table_join->strictness = JoinStrictness::Any;
            break;
        case QJoinKind::Equi:
            table_join->kind = JoinKind::Inner;
            table_join->strictness = JoinStrictness::All;
            break;
        case QJoinKind::AsOf:
            table_join->kind = JoinKind::Left;
            table_join->strictness = JoinStrictness::Asof;
            break;
    }

    ASTs conditions;
    for (size_t i = 0; i < keys.size(); ++i)
    {
        /// The keys are matched by the rules of q: a null key matches a null key.
        bool asof_key = kind == QJoinKind::AsOf && i + 1 == keys.size();
        conditions.push_back(fn(asof_key ? "greaterOrEquals" : "isNotDistinctFrom", qualified(left_alias, keys[i]), qualified(right_alias, keys[i])));
    }
    ASTPtr condition = conditions.size() == 1 ? conditions[0] : fn("and", conditions);
    table_join->on_expression = condition;
    table_join->children.push_back(condition);
    builder.joins.emplace_back(makeSubqueryTableExpression(right_marked.build(), right_alias), table_join);

    std::unordered_set<String> key_set(keys.begin(), keys.end());
    ASTPtr matched = fn("equals", qualified(right_alias, match), one());
    for (const auto & column : left.columns)
    {
        /// The keys are equal in the matched rows, except the last key of an as-of join: the matched row of the right
        /// table is the last one that is not after the row of the left table. `aj` keeps the value of the left table,
        /// and `aj0` takes the one of the matched row.
        bool from_left = key_set.contains(column.name) && !(asof_key_from_right && column.name == keys.back());
        if (!from_left && hasColumn(right, column.name))
            builder.select.push_back(withAlias(fn("if", matched, qualified(right_alias, column.name), qualified(left_alias, column.name)), column.name));
        else
            builder.select.push_back(withAlias(qualified(left_alias, column.name), column.name));
    }
    for (const auto & column : right.columns)
    {
        if (hasColumn(left, column.name))
            continue;
        if (kind == QJoinKind::Left || kind == QJoinKind::AsOf)
            builder.select.push_back(withAlias(fn("if", matched, qualified(right_alias, column.name), lit(Field())), column.name));
        else
            builder.select.push_back(withAlias(qualified(right_alias, column.name), column.name));
    }
    return tableValue(call.compiler, builder.build(), left.keys);
}

/// The values of a table literal for `meta`.
QValue metaTable(Call & call, const QTable & table)
{
    ASTs names;
    ASTs types;
    ASTs foreign;
    ASTs attributes;
    for (const auto & column : table.columns)
    {
        names.push_back(lit(Field(column.name)));
        bool is_string = false;
        size_t depth = arrayDepth(column.type, is_string);
        auto qtype = dataTypeToQType(column.type);
        char code = qtype ? qTypeChar(*qtype) : ' ';
        if (is_string && depth == 0 && qtype == QType::Symbol)
            code = 's';
        if (depth == 1)
            code = static_cast<char>(toupper(code));
        types.push_back(lit(Field(String(1, code))));
        foreign.push_back(lit(Field(String())));
        attributes.push_back(lit(Field(String())));
    }
    QSelectBuilder inner;
    inner.select.push_back(withAlias(fn("array", names), "c"));
    inner.select.push_back(withAlias(fn("array", types), "t"));
    inner.select.push_back(withAlias(fn("array", foreign), "f"));
    inner.select.push_back(withAlias(fn("array", attributes), "a"));
    QSelectBuilder outer;
    outer.from = makeSubqueryTableExpression(inner.build());
    outer.select = {ident("c"), ident("t"), ident("f"), ident("a")};
    outer.array_join = {ident("c"), ident("t"), ident("f"), ident("a")};
    return tableValue(call.compiler, outer.build(), {"c"});
}

QValue countValue(Call & call)
{
    call.expectArgs(1);
    if (call.arg(0).kind == QValueKind::Table)
    {
        QSelectBuilder builder;
        builder.from = call.arg(0).table->table_expression;
        builder.select.push_back(fn("count", ASTs{}));
        QValue result = QCompiler::scalar(make_intrusive<ASTSubquery>(builder.build()));
        result.qtype = QType::Long;
        return result;
    }
    if (call.arg(0).kind == QValueKind::Dictionary)
        return QCompiler::scalar(fn("length", call.arg(0).dictionary->first.ast));
    return aggregate(call, [](ASTPtr) { return fn("count", ASTs{}); }, [](ASTPtr list) { return fn("length", std::move(list)); }, true, QType::Long);
}

QValue firstOrLast(Call & call, bool first)
{
    call.expectArgs(1);
    if (call.arg(0).kind == QValueKind::Table)
        return takeRows(call, QCompiler::scalar(lit(Field(static_cast<Int64>(first ? 1 : -1)))), *call.arg(0).table);
    QValue value = call.scalarArg(0);
    if (value.kind == QValueKind::Scalar && value.is_string && value.depth == 0)
    {
        QValue result = value;
        result.ast = first ? fn("substring", value.ast, one(), one()) : fn("substring", value.ast, lit(Field(static_cast<Int64>(-1))), one());
        return result;
    }
    if (value.kind == QValueKind::Column && call.scope)
        call.scope->uses_row_number = true;
    QValue result = aggregate(call,
        [&](ASTPtr column) { return QCompiler::pickFromColumn(first ? "argMin" : "argMax", std::move(column), ident("__qrow")); },
        [&](ASTPtr list) { return fn("arrayElement", std::move(list), lit(Field(static_cast<Int64>(first ? 1 : -1)))); });
    result.is_string = value.is_string;
    return result;
}

/// The values of a column as a list: a column of a q-sql template is a list (one per group).
/// It takes the column itself, not the arguments of the call, so that it suits the functions of two arguments as well.
QValue columnToList(Call & call, const QValue & column)
{
    if (column.depth > 0)
        call.unsupported("This function of a column of lists");
    QValue result = QCompiler::scalar(call.compiler.collectColumn(call.compiler.columnForAggregation(column, call.scope)), 1);
    result.aggregated = true;
    result.qtype = column.qtype;
    return result;
}

/// The list functions that make a list per group from a column (`distinct`, `asc`, ...).
QValue listFunction(Call & call, const std::function<ASTPtr(ASTPtr)> & over_list)
{
    call.expectArgs(1);
    QValue value = call.scalarArg(0);
    if (value.kind == QValueKind::Column)
    {
        QValue result = columnToList(call, value);
        result.ast = over_list(result.ast);
        return result;
    }
    if (value.depth == 0)
        return value;
    QValue result = value;
    result.ast = over_list(value.ast);
    return result;
}

QValue joinValues(Call & call)
{
    call.expectArgs(2);
    QValue & left = call.arg(0);
    QValue & right = call.arg(1);
    if (left.kind == QValueKind::Table && right.kind == QValueKind::Table)
        return appendTables(call, *left.table, *right.table);

    QValue a = call.scalarArg(0);
    QValue b = call.scalarArg(1);
    if (a.kind == QValueKind::Column || b.kind == QValueKind::Column)
        call.unsupported("Joining columns (use a list per row with 'enlist' or join tables)");

    if (a.is_string && b.is_string && a.depth == 0 && b.depth == 0)
    {
        QValue result = a;
        result.ast = fn("concat", a.ast, b.ast);
        result.aggregated = a.aggregated || b.aggregated;
        return result;
    }

    size_t depth = std::max<size_t>({1, a.depth, b.depth});
    auto lift = [&](QValue value)
    {
        while (value.depth + 1 < depth + (value.depth == 0 ? 0 : 1) && value.depth < depth)
        {
            if (value.depth + 1 == depth || value.depth == 0)
            {
                value.ast = fn("array", value.ast);
                value.depth += 1;
                if (value.depth == depth)
                    break;
            }
            else
                break;
        }
        if (value.depth < depth)
        {
            value.ast = fn("array", value.ast);
            value.depth += 1;
        }
        return value;
    };
    a = lift(a);
    b = lift(b);
    if (a.depth != b.depth)
        call.unsupported("Joining lists of different depths");
    QValue result = QCompiler::scalar(fn("arrayConcat", a.ast, b.ast), depth);
    result.aggregated = a.aggregated || b.aggregated;
    result.qtype = a.qtype == b.qtype ? a.qtype : std::nullopt;
    return result;
}

QValue takeValue(Call & call)
{
    call.expectArgs(2);
    if (call.arg(1).kind == QValueKind::Table)
    {
        if (auto columns = constantSymbols(call.arg(0)); columns && call.arg(0).qtype == QType::Symbol)
        {
            /// `` `a`b#t ``: the columns of a table.
            const QTable & table = *call.arg(1).table;
            QSelectBuilder builder;
            builder.from = table.table_expression;
            for (const auto & column : *columns)
            {
                if (!hasColumn(table, column))
                    call.fail(ErrorCodes::UNKNOWN_IDENTIFIER, fmt::format("unknown column '{}'", column));
                builder.select.push_back(ident(column));
            }
            return tableValue(call.compiler, builder.build());
        }
        return takeRows(call, call.scalarArg(0), *call.arg(1).table);
    }

    QValue count = call.scalarArg(0);
    QValue value = call.scalarArg(1);
    if (count.depth != 0)
        call.unsupported("Reshaping");
    auto n = constantInt(count);

    if (value.kind == QValueKind::Column)
    {
        /// `2#x`: the items are taken from the list of the column.
        QValue list = columnToList(call, value);
        call.args = {count, list};
        return takeValue(call);
    }

    /// The number of the items taken. A negative count takes them from the end. The items repeat when there are fewer
    /// of them than taken, in both directions: `5#1 2` is `1 2 1 2 1`, and `-5#1 2` is `2 1 2 1 2`, the last five items
    /// of the repeated list.
    if (n && *n == std::numeric_limits<Int64>::min())
        call.fail(ErrorCodes::BAD_ARGUMENTS, "the count is too large");
    ASTPtr size = n ? lit(Field(static_cast<UInt64>(*n < 0 ? -*n : *n))) : fn("abs", count.ast);
    auto from_the_end = [&](ASTPtr if_negative, ASTPtr otherwise)
    {
        if (n)
            return *n < 0 ? if_negative : otherwise;
        return fn("if", fn("less", count.ast, lit(Field(static_cast<UInt64>(0)))), if_negative, otherwise);
    };

    if (value.is_string && value.depth == 0)
    {
        /// The string repeated to have at least `size` characters, and its first or last `size` characters.
        ASTPtr length = fn("length", value.ast);
        ASTPtr repeated = fn("repeat", value.ast, fn("toUInt64", fn("intDivOrZero", fn("plus", size, fn("minus", length, one())), length)));
        QValue result = value;
        result.ast = from_the_end(
            fn("substring", repeated, fn("plus", fn("minus", fn("length", repeated), size), one()), size),
            fn("substring", repeated, one(), size));
        result.aggregated |= count.aggregated;
        return result;
    }

    if (value.depth == 0)
    {
        QValue result = QCompiler::scalar(fn("arrayWithConstant", size, value.ast), 1);
        result.qtype = value.qtype;
        result.aggregated = value.aggregated || count.aggregated;
        return result;
    }

    /// The item at `(start + index) % length`, where `start` is zero, or for a negative count the first of the last
    /// `size` items of the repeated list: `(length - size % length) % length`.
    QValue result = value;
    String index = call.compiler.newLambdaParameter();
    ASTPtr length = fn("length", value.ast);
    ASTPtr zero = lit(Field(static_cast<UInt64>(0)));
    ASTPtr start = from_the_end(fn("moduloOrZero", fn("minus", length, fn("moduloOrZero", size, length)), length), zero);
    result.ast = fn("arrayMap",
        makeLambda({index}, fn("arrayElement", value.ast, fn("plus", fn("modulo", fn("plus", ident(index), start), length), one()))),
        fn("range", size));
    result.aggregated |= count.aggregated;
    return result;
}

QValue dropValue(Call & call)
{
    call.expectArgs(2);
    if (call.arg(1).kind == QValueKind::Table)
    {
        const QTable & table = *call.arg(1).table;
        QSelectBuilder builder = selectColumns(table);
        builder.offset = call.scalarArg(0).ast;
        builder.limit = lit(Field(std::numeric_limits<UInt64>::max() / 2));
        return tableValue(call.compiler, builder.build(), table.keys);
    }
    QValue count = call.scalarArg(0);
    QValue value = call.scalarArg(1);
    if (count.depth != 0 || value.kind == QValueKind::Column)
        call.unsupported("This form of drop");
    auto n = constantInt(count);
    QValue result = value;
    if (value.is_string && value.depth == 0)
    {
        if (n && *n < 0)
            result.ast = fn("substring", value.ast, one(), fn("greatest", fn("minus", fn("length", value.ast), lit(Field(-*n))), lit(Field(static_cast<Int64>(0)))));
        else
            result.ast = fn("substring", value.ast, fn("plus", count.ast, one()));
        return result;
    }
    if (value.depth == 0)
        call.unsupported("Dropping from an atom");
    if (n && *n < 0)
        result.ast = fn("arraySlice", value.ast, one(), fn("greatest", fn("minus", fn("length", value.ast), lit(Field(-*n))), lit(Field(static_cast<Int64>(0)))));
    else
        result.ast = fn("arraySlice", value.ast, fn("plus", count.ast, one()));
    return result;
}

QValue keyTable(Call & call, const Strings & keys, const QTable & table)
{
    /// The key columns move to the front.
    QSelectBuilder builder;
    builder.from = table.table_expression;
    for (const auto & key : keys)
    {
        if (!hasColumn(table, key))
            call.fail(ErrorCodes::UNKNOWN_IDENTIFIER, fmt::format("unknown column '{}'", key));
        builder.select.push_back(ident(key));
    }
    std::unordered_set<String> key_set(keys.begin(), keys.end());
    for (const auto & column : table.columns)
        if (!key_set.contains(column.name))
            builder.select.push_back(ident(column.name));
    return tableValue(call.compiler, builder.build(), Names(keys.begin(), keys.end()));
}

QValue bangValue(Call & call)
{
    call.expectArgs(2);
    if (call.arg(1).kind == QValueKind::Table)
    {
        auto n = constantInt(call.arg(0));
        if (!n || *n < 0)
            call.unsupported("This form of '!' with a table");
        const QTable & table = *call.arg(1).table;
        if (static_cast<size_t>(*n) > table.columns.size())
            call.fail(ErrorCodes::BAD_ARGUMENTS, "too many key columns");
        Strings keys;
        size_t i = 0;
        for (const auto & column : table.columns)
            if (i++ < static_cast<size_t>(*n))
                keys.push_back(column.name);
        auto result = std::make_shared<QTable>(table);
        result->keys = Names(keys.begin(), keys.end());
        QValue value = call.arg(1);
        value.table = std::move(result);
        return value;
    }

    QValue keys = call.scalarArg(0);
    QValue values = call.scalarArg(1);
    if (keys.kind != QValueKind::Scalar || values.kind != QValueKind::Scalar || keys.depth == 0)
    {
        if (keys.depth == 0 && values.depth == 0 && keys.kind == QValueKind::Scalar && values.kind == QValueKind::Scalar)
        {
            /// `` `a!1 ``: a dictionary of one item.
            keys.ast = fn("array", keys.ast);
            keys.depth = 1;
            values.ast = fn("array", values.ast);
            values.depth = 1;
        }
        else
            call.unsupported("This form of '!'");
    }
    if (values.depth == 0)
    {
        values.ast = fn("arrayWithConstant", fn("length", keys.ast), values.ast);
        values.depth = 1;
    }
    QValue result;
    result.kind = QValueKind::Dictionary;
    result.dictionary = std::make_shared<std::pair<QValue, QValue>>(keys, values);
    return result;
}

QValue castOrCond(Call & call)
{
    if (call.args.size() >= 3)
    {
        /// `$[c;a;b]`, `$[c1;a1;c2;a2;else]`.
        ASTs asts;
        QValues values;
        for (size_t i = 0; i < call.args.size(); ++i)
            values.push_back(call.scalarArg(i));
        return call.compiler.atomic(call.name, values, [](const ASTs & items) { return fn("multiIf", items); }, call.scope);
    }

    call.expectArgs(2);
    QValue target = call.scalarArg(0);
    QValue value = call.scalarArg(1);
    if (auto name = constantString(target); name && (target.qtype == QType::Symbol || target.qtype == QType::Char))
        return castValue(call, *name, value);
    if (auto n = constantInt(target); n && value.is_string)
    {
        /// `5$"abc"`: pad or truncate a string.
        QValue result = value;
        if (*n >= 0)
            result.ast = fn("rightPad", value.ast, lit(Field(*n)), lit(Field(String(" "))));
        else
            result.ast = fn("leftPad", value.ast, lit(Field(-*n)), lit(Field(String(" "))));
        return result;
    }
    call.unsupported("This form of '$'");
}

QValue movingWindow(Call & call, const String & function)
{
    call.expectArgs(2);
    auto n = constantInt(call.arg(0));
    if (!n || *n <= 0)
        call.fail(ErrorCodes::BAD_ARGUMENTS, "the window size must be a positive integer literal");
    UInt64 size = static_cast<UInt64>(*n);
    return uniform(call,
        [&](ASTPtr column)
        {
            return call.compiler.makeWindowFunction(function, {column}, call.scope, size - 1, 0);
        },
        [&](ASTPtr list)
        {
            /// The moving sum takes a null for zero, like the running one.
            if (function == "sum")
                list = nullsAsZeros(call.compiler, std::move(list));
            return reduceInRanges(call.compiler, function, list,
                [&](ASTPtr index)
                {
                    /// The window ends at the item: `(start, index - start + 1)`. The length must be unsigned.
                    ASTPtr start = fn("greatest", one(), fn("minus", index, lit(Field(size - 1))));
                    return fn("tuple", start, fn("toUInt64", fn("plus", fn("minus", index, start), one())));
                });
        });
}

QValue xbar(Call & call)
{
    call.expectArgs(2);
    QValue step = call.scalarArg(0);
    QValue value = call.scalarArg(1);
    bool temporal = value.qtype == QType::Timestamp || value.qtype == QType::Datetime || value.qtype == QType::Date || value.qtype == QType::Month;
    bool clock = value.qtype == QType::Minute || value.qtype == QType::Second || value.qtype == QType::Time || value.qtype == QType::Timespan;

    /// A month, a date, and a timestamp are rounded as the counts of their units since 2000.01.01, like in q:
    /// `7 xbar 2000.01m` is 2000.01m, the month zero. (`toStartOfInterval` counts the months from 1900 and the days and
    /// the nanoseconds from 1970, so its buckets are other ones when the width does not divide those offsets.)
    bool step_is_clock = step.qtype == QType::Timespan || step.qtype == QType::Minute || step.qtype == QType::Second || step.qtype == QType::Time;
    bool counted = value.qtype == QType::Timestamp || ((value.qtype == QType::Month || value.qtype == QType::Date) && !step_is_clock);
    if (counted)
    {
        QType temporal_type = *value.qtype;
        return binary(call,
            [&](ASTPtr width, ASTPtr x)
            {
                /// A width that is a time is a number of nanoseconds for a timestamp.
                if (step_is_clock)
                    width = fn("toInt64", fn("multiply", fn("toFloat64", std::move(width)), lit(Field(static_cast<UInt64>(1'000'000'000)))));
                ASTPtr count = temporalToNumber(std::move(x), temporal_type, call);
                return numberToTemporal(fn("minus", count, fn("positiveModulo", count, std::move(width))), temporal_type, call);
            },
            temporal_type);
    }

    if (temporal)
    {
        ASTPtr interval;
        if (step.qtype == QType::Timespan || step.qtype == QType::Minute || step.qtype == QType::Second || step.qtype == QType::Time)
            interval = fn("toIntervalNanosecond", fn("toInt64", fn("multiply", fn("toFloat64", step.ast), lit(Field(static_cast<UInt64>(1'000'000'000))))));
        else if (value.qtype == QType::Month)
            interval = fn("toIntervalMonth", step.ast);
        else if (value.qtype == QType::Date)
            interval = fn("toIntervalDay", step.ast);
        else
            interval = fn("toIntervalNanosecond", step.ast);
        QValue result = value;
        result.ast = fn("toStartOfInterval", value.ast, interval);
        result.aggregated |= step.aggregated;
        return result;
    }

    if (clock)
    {
        /// Times of day are compiled into `Time` and `Time64`: round the seconds.
        Int64 unit_seconds_numerator = value.qtype == QType::Minute ? 60 : 1;
        Int64 unit_seconds_divisor = value.qtype == QType::Time ? 1000 : (value.qtype == QType::Timespan ? 1'000'000'000 : 1);
        ASTPtr step_seconds;
        if (step.qtype == QType::Timespan || step.qtype == QType::Minute || step.qtype == QType::Second || step.qtype == QType::Time)
            step_seconds = fn("toFloat64", step.ast);
        else
            step_seconds = fn("divide", fn("multiply", step.ast, lit(Field(unit_seconds_numerator))), lit(Field(unit_seconds_divisor)));
        ASTPtr seconds = fn("toFloat64", value.ast);
        ASTPtr rounded = fn("multiply", fn("floor", fn("divide", seconds, step_seconds)), step_seconds);
        QValue result = value;
        String type = value.qtype == QType::Time ? "Time64(3)" : (value.qtype == QType::Timespan ? "Time64(9)" : "Time");
        result.ast = fn("CAST", rounded, lit(Field(type)));
        return result;
    }

    return binary(call, [](ASTPtr n, ASTPtr x) { return fn("minus", x, fn("positiveModulo", x, n)); });
}

QValue inValue(Call & call)
{
    call.expectArgs(2);
    QValue value = call.scalarArg(0);
    if (call.arg(1).kind == QValueKind::Table)
    {
        const QTable & table = *call.arg(1).table;
        if (table.columns.size() != 1)
            call.fail(ErrorCodes::TYPE_MISMATCH, "the table must have one column");
        QSelectBuilder builder = selectColumns(table);
        auto subquery = make_intrusive<ASTSubquery>(builder.build());
        QValue result = call.compiler.atomic(call.name, {value}, [&](const ASTs & asts) { return fn("in", asts[0], subquery); }, call.scope);
        result.qtype = QType::Boolean;
        return result;
    }
    QValue list = call.scalarArg(1);
    if (list.depth == 0)
        return binaryFunction(call, "equals", QType::Boolean);
    if (list.kind == QValueKind::Column)
        call.unsupported("'in' with a column on the right");
    QValue result = call.compiler.atomic(call.name, {value}, [&](const ASTs & asts) { return fn("has", list.ast, asts[0]); }, call.scope);
    result.aggregated |= list.aggregated;
    result.qtype = QType::Boolean;
    return result;
}

QValue sqlTable(Call & call)
{
    call.expectArgs(1);
    auto text = constantString(call.arg(0));
    if (!text)
        call.fail(ErrorCodes::BAD_ARGUMENTS, "expected a string with a ClickHouse query");
    const auto & settings = call.compiler.getContext()->getSettingsRef();
    ParserQuery parser(text->data() + text->size());
    ASTPtr query = parseQuery(parser, text->data(), text->data() + text->size(), "", settings[Setting::max_query_size],
        settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);
    if (!query->as<ASTSelectWithUnionQuery>())
        call.fail(ErrorCodes::BAD_ARGUMENTS, "expected a SELECT query");

    /// The query parameters of the request (`{name:Type}`) are substituted here: the structure of the query is analyzed
    /// right away, and the analysis does not take the parameters. The substitution of the parameters in the compiled
    /// query, which is done for every query, comes too late for that.
    ReplaceQueryParameterVisitor visitor(call.compiler.getContext()->getQueryParameters());
    visitor.visit(query);

    return tableValue(call.compiler, query);
}

/// `where` of a boolean list: the indices of the true items.
QValue h_where(Call & c)
{
    c.expectArgs(1);
    QValue value = c.scalarArg(0);
    if (value.kind == QValueKind::Column)
        c.unsupported("where of a column (use the where phrase)");
    if (value.depth != 1)
        c.fail(ErrorCodes::TYPE_MISMATCH, "expected a boolean list");
    String index = c.compiler.newLambdaParameter();
    String item = c.compiler.newLambdaParameter();
    String position = c.compiler.newLambdaParameter();
    ASTPtr selected = fn("arrayFilter", makeLambda({index, item}, ident(item)), fn("arrayEnumerate", value.ast), value.ast);
    QValue result = value;
    result.ast = fn("arrayMap", makeLambda({position}, fn("minus", ident(position), one())), selected);
    result.qtype = QType::Long;
    return result;
}

bool isClockType(std::optional<QType> type)
{
    return type == QType::Timespan || type == QType::Time || type == QType::Second || type == QType::Minute;
}

bool isDateTimeType(std::optional<QType> type)
{
    return type == QType::Timestamp || type == QType::Datetime;
}

/// The count of seconds of a temporal value as a decimal with nine digits after the point. It is exact for every
/// temporal type, while a `Float64` cannot tell two adjacent nanoseconds of a present-day timestamp apart.
ASTPtr exactSeconds(ASTPtr value, std::optional<QType> type)
{
    auto scale = [] { return lit(Field(static_cast<UInt64>(9))); };
    /// `DateTime` or `DateTime64` of any scale.
    if (isDateTimeType(type))
        return fn("toDecimal128", fn("toDateTime64", std::move(value), scale()), scale());
    /// `Time` is a count of whole seconds.
    if (type == QType::Minute || type == QType::Second)
        return fn("toDecimal128", fn("toInt64", std::move(value)), scale());
    /// `Time64` of any scale.
    return fn("toDecimal128", std::move(value), scale());
}

/// Arithmetic of times of day and durations, which ClickHouse does not have for `Time` and `Time64`:
/// `10:30:00.000 + 00:00:01.500`, and the difference of two timestamps, which is a timespan.
std::optional<QValue> clockArithmetic(Call & call, const String & function)
{
    if (call.args.size() != 2)
        return std::nullopt;
    std::optional<QType> left = call.args[0].qtype;
    std::optional<QType> right = call.args[1].qtype;
    /// A temporal value and a number: the number counts the units of the temporal type, as in a cast. `2024.01m+1` is
    /// the next month and a timestamp plus one is the next nanosecond, while `plus` of ClickHouse would add a day to
    /// the date that a month is, and a second to the timestamp. The result has the temporal type.
    bool left_is_temporal = left && isTemporalType(*left);
    bool right_is_temporal = right && isTemporalType(*right);
    bool left_is_number = left && isNumberType(*left) && !call.args[0].is_string;
    bool right_is_number = right && isNumberType(*right) && !call.args[1].is_string;
    if ((left_is_temporal && right_is_number) || (function == "plus" && left_is_number && right_is_temporal))
    {
        bool temporal_first = left_is_temporal;
        QType temporal_type = temporal_first ? *left : *right;
        return binary(call,
            [&](ASTPtr a, ASTPtr b)
            {
                ASTPtr count = temporalToNumber(temporal_first ? a : b, temporal_type, call);
                ASTPtr number = temporal_first ? b : a;
                ASTPtr sum = temporal_first ? fn(function, std::move(count), std::move(number)) : fn(function, std::move(number), std::move(count));
                return numberToTemporal(std::move(sum), temporal_type, call);
            },
            temporal_type);
    }

    /// The difference of two months is a number of months, and of two dates a number of days.
    /// (`minus` of ClickHouse does not take two dates, and a month is a date there.)
    if (function == "minus" && left && left == right && (*left == QType::Month || *left == QType::Date))
    {
        QType temporal_type = *left;
        return binary(call,
            [&](ASTPtr a, ASTPtr b)
            {
                return fn("minus", temporalToNumber(std::move(a), temporal_type, call), temporalToNumber(std::move(b), temporal_type, call));
            },
            QType::Int);
    }

    std::optional<QType> result_type;
    if (isClockType(left) && isClockType(right))
    {
        /// The more precise of the two types, whatever the order of the operands is: a minute and a second make a second.
        auto precision = [](QType type)
        {
            switch (type)
            {
                case QType::Timespan: return 3;
                case QType::Time: return 2;
                case QType::Second: return 1;
                default: return 0;
            }
        };
        result_type = precision(*left) >= precision(*right) ? *left : *right;
    }
    else if (function == "minus" && isDateTimeType(left) && isDateTimeType(right))
        result_type = QType::Timespan;
    else
        return std::nullopt;
    String type = qTypeToClickHouseType(*result_type);
    /// The result in `Time` is a count of whole seconds; in `Time64` it is converted from the decimal as it is.
    bool whole_seconds = result_type == QType::Minute || result_type == QType::Second;
    QValue result = binary(call,
        [&](ASTPtr a, ASTPtr b)
        {
            ASTPtr seconds = fn(function, exactSeconds(std::move(a), left), exactSeconds(std::move(b), right));
            if (whole_seconds)
                seconds = fn("toInt64", std::move(seconds));
            return fn("CAST", std::move(seconds), lit(Field(type)));
        },
        result_type);
    return result;
}

/// `aj[keys; t1; t2]` and `aj0[keys; t1; t2]`: for each row of `t1`, the last row of `t2` with the same keys
/// that is not after it by the last key.
QValue asOfJoin(Call & call, bool key_from_right)
{
    call.expectArgs(3);
    auto keys = constantSymbols(call.arg(0));
    if (!keys)
        call.fail(ErrorCodes::TYPE_MISMATCH, "the first argument must be the symbols of the key columns");
    return joinTables(call, QJoinKind::AsOf, call.tableArg(1), call.tableArg(2), Names(keys->begin(), keys->end()), key_from_right);
}

const std::unordered_map<String, Handler> & handlers()
{
    static const std::unordered_map<String, Handler> map = []
    {
        std::unordered_map<String, Handler> h;

        /// Arithmetic and comparison.
        h["+"] = [](Call & c)
        {
            if (c.args.size() == 1)
                c.unsupported("flip");
            if (auto result = clockArithmetic(c, "plus"))
                return *result;
            return binaryFunction(c, "plus");
        };
        h["-"] = [](Call & c)
        {
            if (c.args.size() == 1)
                return unary(c, "negate", std::nullopt, true);
            if (auto result = clockArithmetic(c, "minus"))
                return *result;
            return binaryFunction(c, "minus");
        };
        h["*"] = [](Call & c)
        {
            if (c.args.size() == 1)
                return firstOrLast(c, true);
            return binaryFunction(c, "multiply");
        };
        h["%"] = [](Call & c)
        {
            if (c.args.size() == 1)
                return handlers().at("reciprocal")(c);
            return binaryFunction(c, "divide", QType::Float);
        };
        h["&"] = [](Call & c)
        {
            if (c.args.size() == 1)
                return h_where(c);
            if (c.arg(0).qtype == QType::Boolean && c.arg(1).qtype == QType::Boolean)
                return binaryFunction(c, "and", QType::Boolean);

            /// A null is less than every other value in q, so the lesser of a null and a value is the null, while
            /// `least` of ClickHouse skips the nulls. (The greater of them is the value in both.)
            if (c.compiler.isNotNull(c.arg(0).ast) && c.compiler.isNotNull(c.arg(1).ast))
                return binaryFunction(c, "least");
            return binary(c,
                [](ASTPtr x, ASTPtr y)
                {
                    return fn("if", fn("or", fn("isNull", x), fn("isNull", y)), lit(Field()), fn("least", x, y));
                });
        };
        h["|"] = [](Call & c)
        {
            if (c.args.size() == 1)
                return listFunction(c, [](ASTPtr list) { return fn("arrayReverse", list); });
            if (c.arg(0).qtype == QType::Boolean && c.arg(1).qtype == QType::Boolean)
                return binaryFunction(c, "or", QType::Boolean);
            return binaryFunction(c, "greatest");
        };
        h["="] = [](Call & c) { return comparison(c, "equals"); };
        h["<>"] = [](Call & c) { return comparison(c, "notEquals"); };
        h["<"] = [](Call & c)
        {
            if (c.args.size() == 1)
                return handlers().at("iasc")(c);
            return comparison(c, "less");
        };
        h[">"] = [](Call & c)
        {
            if (c.args.size() == 1)
                return handlers().at("idesc")(c);
            return comparison(c, "greater");
        };
        h["<="] = [](Call & c) { return comparison(c, "lessOrEquals"); };
        h[">="] = [](Call & c) { return comparison(c, "greaterOrEquals"); };
        h["~"] = [](Call & c)
        {
            if (c.args.size() == 1)
                return unary(c, "not", QType::Boolean);
            c.expectArgs(2);
            QValue a = c.scalarArg(0);
            QValue b = c.scalarArg(1);

            /// Match is strict about the types, unlike `=`: `1~1h` is false. So when the q types of both values are known
            /// exactly and differ, or the values are lists of different depths, they do not match, whatever the values
            /// are. A column matches as the list of its values, and by the values alone: its q type is not exact
            /// (see `QValue::qtype_is_exact`).
            size_t depth_of_a = a.depth + (a.kind == QValueKind::Column ? 1 : 0);
            size_t depth_of_b = b.depth + (b.kind == QValueKind::Column ? 1 : 0);
            bool types_differ = a.qtype && b.qtype && a.qtype_is_exact && b.qtype_is_exact && *a.qtype != *b.qtype;
            if (types_differ || depth_of_a != depth_of_b)
            {
                QValue result = QCompiler::scalar(lit(Field(static_cast<UInt64>(0))));
                result.qtype = QType::Boolean;
                return result;
            }

            if (a.kind == QValueKind::Column || b.kind == QValueKind::Column)
            {
                ASTPtr left = a.kind == QValueKind::Column ? c.compiler.collectColumn(c.compiler.columnForAggregation(a, c.scope)) : a.ast;
                ASTPtr right = b.kind == QValueKind::Column ? c.compiler.collectColumn(c.compiler.columnForAggregation(b, c.scope)) : b.ast;
                QValue result = QCompiler::scalar(fn("isNotDistinctFrom", left, right));
                result.aggregated = true;
                result.qtype = QType::Boolean;
                return result;
            }
            QValue result = QCompiler::scalar(fn("isNotDistinctFrom", a.ast, b.ast));
            result.aggregated = a.aggregated || b.aggregated;
            result.qtype = QType::Boolean;
            return result;
        };
        h["^"] = [](Call & c)
        {
            if (c.args.size() == 1)
                return handlers().at("null")(c);
            return binary(c, [](ASTPtr fill, ASTPtr x) { return fn("ifNull", x, fill); });
        };
        h[","] = [](Call & c)
        {
            if (c.args.size() == 1)
                return handlers().at("enlist")(c);
            return joinValues(c);
        };
        h["#"] = [](Call & c)
        {
            if (c.args.size() == 1)
                return countValue(c);
            return takeValue(c);
        };
        h["_"] = [](Call & c)
        {
            if (c.args.size() == 1)
                return unary(c, "floor", QType::Long);
            return dropValue(c);
        };
        h["!"] = [](Call & c)
        {
            if (c.args.size() == 1)
                return handlers().at("til")(c);
            return bangValue(c);
        };
        h["$"] = [](Call & c)
        {
            if (c.args.size() == 1)
                return handlers().at("string")(c);
            return castOrCond(c);
        };
        h["?"] = [](Call & c)
        {
            if (c.args.size() == 1)
                return handlers().at("distinct")(c);
            if (c.args.size() == 3)
                return c.compiler.atomic(c.name, {c.scalarArg(0), c.scalarArg(1), c.scalarArg(2)},
                    [](const ASTs & asts) { return fn("if", asts[0], asts[1], asts[2]); }, c.scope);
            c.expectArgs(2);
            QValue list = c.scalarArg(0);
            QValue item = c.scalarArg(1);
            if (list.depth == 0 || list.kind == QValueKind::Column)
                c.unsupported("This form of '?' (random numbers or find in a column)");
            QValue result = c.compiler.atomic(c.name, {item},
                [&](const ASTs & asts)
                {
                    ASTPtr position = fn("indexOf", list.ast, asts[0]);
                    return fn("if", fn("equals", position, lit(Field(static_cast<UInt64>(0)))), fn("length", list.ast), fn("minus", position, one()));
                },
                c.scope);
            result.qtype = QType::Long;
            return result;
        };
        h["@"] = [](Call & c)
        {
            if (c.args.size() == 1)
                c.unsupported("type");
            c.expectArgs(2);
            return c.compiler.applyValue(c.arg(0), {c.arg(1)}, c.env, c.scope, c.offset);
        };
        h["."] = [](Call & c)
        {
            c.expectArgs(2);
            QValue list = c.scalarArg(1);
            const auto * array = list.ast->as<ASTFunction>();
            if (!array || array->name != "array" || list.depth == 0)
                c.unsupported("Applying a function to a list that is not a literal");
            QValues items;
            for (const auto & item : array->arguments->children)
            {
                QValue value = list;
                value.ast = item;
                value.depth -= 1;
                items.push_back(std::move(value));
            }
            return c.compiler.applyValue(c.arg(0), std::move(items), c.env, c.scope, c.offset);
        };
        h[":"] = [](Call & c)
        {
            c.expectArgs(1);
            return c.arg(0);
        };
        h["::"] = [](Call & c)
        {
            c.expectArgs(1);
            return c.arg(0);
        };

        /// Monadic forms with a colon: `-:x`, `#:x`, ...
        static constexpr std::string_view glyphs = "+-*%&|<>=~,^#_!$?@.";
        for (char glyph : glyphs)
        {
            String name = String(1, glyph) + ":";
            String base(1, glyph);
            h[name] = [base](Call & c)
            {
                c.expectArgs(1);
                return handlers().at(base)(c);
            };
        }

        h["neg"] = [](Call & c) { return unary(c, "negate", std::nullopt, true); };
        h["not"] = [](Call & c) { return unary(c, "not", QType::Boolean); };
        h["abs"] = [](Call & c) { return unary(c, "abs", std::nullopt, true); };
        h["sqrt"] = [](Call & c) { return unary(c, "sqrt", QType::Float); };
        h["exp"] = [](Call & c) { return unary(c, "exp", QType::Float); };
        h["log"] = [](Call & c) { return unary(c, "log", QType::Float); };
        h["sin"] = [](Call & c) { return unary(c, "sin", QType::Float); };
        h["cos"] = [](Call & c) { return unary(c, "cos", QType::Float); };
        h["tan"] = [](Call & c) { return unary(c, "tan", QType::Float); };
        h["asin"] = [](Call & c) { return unary(c, "asin", QType::Float); };
        h["acos"] = [](Call & c) { return unary(c, "acos", QType::Float); };
        h["atan"] = [](Call & c) { return unary(c, "atan", QType::Float); };
        h["floor"] = [](Call & c) { return unary(c, "floor", QType::Long); };
        h["ceiling"] = [](Call & c) { return unary(c, "ceil", QType::Long); };
        h["signum"] = [](Call & c) { return unary(c, "sign", QType::Int); };
        h["reciprocal"] = [](Call & c)
        {
            c.expectArgs(1);
            QValue result = c.compiler.atomic(c.name, {c.scalarArg(0)}, [](const ASTs & asts) { return fn("divide", lit(Field(1.0)), asts[0]); }, c.scope);
            result.qtype = QType::Float;
            return result;
        };
        h["null"] = [](Call & c) { return unary(c, "isNull", QType::Boolean); };
        h["upper"] = [](Call & c) { return unary(c, "upper", std::nullopt, true, true); };
        h["lower"] = [](Call & c) { return unary(c, "lower", std::nullopt, true, true); };
        h["trim"] = [](Call & c) { return unary(c, "trimBoth", std::nullopt, true, true); };
        h["ltrim"] = [](Call & c) { return unary(c, "trimLeft", std::nullopt, true, true); };
        h["rtrim"] = [](Call & c) { return unary(c, "trimRight", std::nullopt, true, true); };
        h["string"] = [](Call & c) { return unary(c, "toString", QType::Char, false, true); };

        /// `and` and `or` are the lesser and the greater of two values, like `&` and `|`: `2 and 3` is 2.
        h["and"] = [](Call & c)
        {
            c.expectArgs(2);
            return handlers().at("&")(c);
        };
        h["or"] = [](Call & c)
        {
            c.expectArgs(2);
            return handlers().at("|")(c);
        };
        h["mod"] = [](Call & c) { return binaryFunction(c, "positiveModulo"); };
        h["div"] = [](Call & c)
        {
            /// The quotient rounded down. `intDiv` is exact for the integers and rounds to zero, so one is subtracted
            /// when there is a remainder and the signs differ. (`floor(x / y)` goes through `Float64`, which does not
            /// hold the integers above 2^53.)
            return binary(c,
                [](ASTPtr x, ASTPtr y)
                {
                    ASTPtr zero = lit(Field(static_cast<UInt64>(0)));
                    ASTPtr rounded_up = fn("and", fn("notEquals", fn("modulo", x, y), zero), fn("notEquals", fn("less", x, zero), fn("less", y, zero)));
                    return fn("minus", fn("intDiv", x, y), rounded_up);
                },
                QType::Long);
        };
        h["xexp"] = [](Call & c) { return binaryFunction(c, "pow", QType::Float); };
        h["xlog"] = [](Call & c)
        {
            return binary(c, [](ASTPtr base, ASTPtr x) { return fn("divide", fn("log", x), fn("log", base)); }, QType::Float);
        };
        h["xbar"] = [](Call & c) { return xbar(c); };
        h["in"] = [](Call & c) { return inValue(c); };
        h["within"] = [](Call & c)
        {
            c.expectArgs(2);
            QValue value = c.scalarArg(0);
            QValue range = c.scalarArg(1);
            if (range.depth != 1 || range.kind != QValueKind::Scalar)
                c.fail(ErrorCodes::TYPE_MISMATCH, "the right argument must be a pair (low; high)");
            QValue result = c.compiler.atomic(c.name, {value},
                [&](const ASTs & asts)
                {
                    return fn("and", fn("greaterOrEquals", asts[0], fn("arrayElement", range.ast, one())),
                        fn("lessOrEquals", asts[0], fn("arrayElement", range.ast, lit(Field(static_cast<UInt64>(2))))));
                },
                c.scope);
            result.qtype = QType::Boolean;
            return result;
        };
        h["like"] = [](Call & c)
        {
            c.expectArgs(2);
            auto pattern = constantString(c.arg(1));
            if (!pattern)
                c.unsupported("A pattern that is not a string literal");
            String regexp = likePatternToRegexp(*pattern);
            QValue result = c.compiler.atomic(c.name, {c.scalarArg(0)},
                [&](const ASTs & asts) { return fn("match", asts[0], lit(Field(regexp))); }, c.scope);
            result.qtype = QType::Boolean;
            result.is_string = false;
            return result;
        };
        h["ssr"] = [](Call & c)
        {
            c.expectArgs(3);
            QValue result = c.compiler.atomic(c.name, {c.scalarArg(0), c.scalarArg(1), c.scalarArg(2)},
                [](const ASTs & asts) { return fn("replaceAll", asts[0], asts[1], asts[2]); }, c.scope);
            result.is_string = true;
            return result;
        };
        h["sv"] = [](Call & c)
        {
            c.expectArgs(2);
            QValue separator = c.scalarArg(0);
            QValue list = c.scalarArg(1);
            if (list.depth == 0 || !separator.is_string)
                c.unsupported("This form of 'sv'");
            QValue result = QCompiler::scalar(fn("arrayStringConcat", list.ast, separator.ast), list.depth - 1);
            result.is_string = true;
            result.kind = list.kind;
            return result;
        };
        h["vs"] = [](Call & c)
        {
            c.expectArgs(2);
            QValue separator = c.scalarArg(0);
            QValue value = c.scalarArg(1);
            if (!separator.is_string)
                c.unsupported("This form of 'vs'");
            QValue result = c.compiler.atomic(c.name, {value}, [&](const ASTs & asts) { return fn("splitByString", separator.ast, asts[0]); }, c.scope);
            result.depth += 1;
            result.is_string = true;
            return result;
        };
        h["bin"] = [](Call & c)
        {
            c.expectArgs(2);
            QValue list = c.scalarArg(0);
            if (list.depth != 1 || list.kind != QValueKind::Scalar)
                c.fail(ErrorCodes::TYPE_MISMATCH, "the left argument must be a sorted list");
            QValue result = c.compiler.atomic(c.name, {c.scalarArg(1)},
                [&](const ASTs & asts)
                {
                    String item = c.compiler.newLambdaParameter();
                    return fn("minus", fn("arrayCount", makeLambda({item}, fn("lessOrEquals", ident(item), asts[0])), list.ast), one());
                },
                c.scope);
            result.qtype = QType::Long;
            return result;
        };
        h["except"] = [](Call & c)
        {
            c.expectArgs(2);
            QValue left = c.scalarArg(0);
            QValue right = c.scalarArg(1);
            if (left.kind == QValueKind::Column)
                left = columnToList(c, left);
            if (right.kind == QValueKind::Column)
                right = columnToList(c, right);
            if (right.depth == 0)
                right = QCompiler::scalar(fn("array", right.ast), 1);
            String item = c.compiler.newLambdaParameter();
            QValue result = left;
            result.ast = fn("arrayFilter", makeLambda({item}, fn("not", fn("has", right.ast, ident(item)))), left.ast);
            result.aggregated |= right.aggregated;
            return result;
        };
        h["inter"] = [](Call & c)
        {
            c.expectArgs(2);
            QValue left = c.scalarArg(0);
            QValue right = c.scalarArg(1);
            if (left.kind == QValueKind::Column)
                left = columnToList(c, left);
            if (right.kind == QValueKind::Column)
                right = columnToList(c, right);
            String item = c.compiler.newLambdaParameter();
            QValue result = left;
            result.ast = fn("arrayFilter", makeLambda({item}, fn("has", right.ast, ident(item))), left.ast);
            result.aggregated |= right.aggregated;
            return result;
        };
        h["union"] = [](Call & c)
        {
            c.expectArgs(2);
            QValue result = joinValues(c);
            if (result.kind == QValueKind::Table)
            {
                /// The distinct rows of the two tables, in the order of their first occurrence.
                QSelectBuilder builder = selectColumns(*result.table);
                builder.distinct = true;
                return tableValue(c.compiler, builder.build(), result.table->keys);
            }
            result.ast = fn("arrayDistinct", result.ast);
            return result;
        };

        /// Aggregates.
        h["sum"] = [](Call & c) { return simpleAggregate(c, "sum"); };
        h["avg"] = [](Call & c) { return simpleAggregate(c, "avg", QType::Float); };
        h["min"] = [](Call & c) { return simpleAggregate(c, "min"); };
        h["max"] = [](Call & c) { return simpleAggregate(c, "max"); };
        h["med"] = [](Call & c)
        {
            /// The median of q is the mean of the two middle values when the count of the values is even.
            /// `medianExact` takes one of the two, while `quantileExactInclusive` interpolates between them.
            return aggregate(c,
                [](ASTPtr column) -> ASTPtr
                {
                    auto median = make_intrusive<ASTFunction>();
                    median->name = "quantileExactInclusive";
                    median->arguments = makeExpressionList({std::move(column)});
                    median->children.push_back(median->arguments);
                    median->parameters = makeExpressionList({lit(Field(0.5))});
                    median->children.push_back(median->parameters);
                    return median;
                },
                [](ASTPtr list) { return fn("arrayReduce", lit(Field(String("quantileExactInclusive(0.5)"))), std::move(list)); },
                false, QType::Float);
        };
        h["dev"] = [](Call & c) { return simpleAggregate(c, "stddevPop", QType::Float); };
        h["sdev"] = [](Call & c) { return simpleAggregate(c, "stddevSamp", QType::Float); };
        h["var"] = [](Call & c) { return simpleAggregate(c, "varPop", QType::Float); };
        h["svar"] = [](Call & c) { return simpleAggregate(c, "varSamp", QType::Float); };
        h["prd"] = [](Call & c)
        {
            return aggregate(c, [](ASTPtr column) { return fn("arrayProduct", fn("groupArray", column)); },
                [&](ASTPtr list) { return fn("arrayProduct", withoutNulls(c.compiler, std::move(list))); });
        };
        /// `all` and `any` test whether the values are not zero, whatever their type is: 256 and 0.5 are true.
        /// A null is not zero. They return a boolean for an atom as well.
        auto all_or_any = [](bool all)
        {
            return [all](Call & c)
            {
                auto truth = [](ASTPtr value) { return fn("ifNull", fn("notEquals", std::move(value), lit(Field(static_cast<UInt64>(0)))), one()); };

                c.expectArgs(1);
                QValue value = c.scalarArg(0);
                if (value.kind != QValueKind::Column && value.depth == 0)
                {
                    /// The characters of a string are not zero, so only `any` of an empty string is false.
                    ASTPtr test = !value.is_string ? truth(value.ast) : (all ? one() : fn("notEmpty", value.ast));
                    QValue result = QCompiler::scalar(fn("toBool", test));
                    result.aggregated = value.aggregated;
                    result.qtype = QType::Boolean;
                    return result;
                }
                return aggregate(c,
                    [&](ASTPtr column) { return fn("toBool", fn(all ? "min" : "max", truth(std::move(column)))); },
                    [&](ASTPtr list)
                    {
                        String item = c.compiler.newLambdaParameter();
                        return fn("toBool", fn(all ? "arrayAll" : "arrayExists", makeLambda({item}, truth(ident(item))), std::move(list)));
                    }, false, QType::Boolean);
            };
        };
        h["all"] = all_or_any(true);
        h["any"] = all_or_any(false);
        h["count"] = [](Call & c) { return countValue(c); };
        h["first"] = [](Call & c) { return firstOrLast(c, true); };
        h["last"] = [](Call & c) { return firstOrLast(c, false); };
        h["wavg"] = [](Call & c)
        {
            c.expectArgs(2);
            QValue weights = c.scalarArg(0);
            QValue values = c.scalarArg(1);
            /// A null in either argument excludes the whole pair. The product of such a pair is a null, and the sum skips it.
            /// The sum of the weights has to skip the same pairs, so a weight is replaced with a null where the value is one.
            QValue product = c.compiler.atomic(c.name, {weights, values}, [](const ASTs & asts) { return fn("multiply", asts[0], asts[1]); }, c.scope);
            QValue paired_weights = c.compiler.atomic(c.name, {weights, values},
                [](const ASTs & asts) { return fn("if", fn("isNull", asts[1]), lit(Field()), asts[0]); }, c.scope);
            QValues numerator_args{product};
            Call numerator_call{c.compiler, c.name, numerator_args, c.env, c.scope, c.offset};
            QValue numerator = simpleAggregate(numerator_call, "sum");
            QValues denominator_args{paired_weights};
            Call denominator_call{c.compiler, c.name, denominator_args, c.env, c.scope, c.offset};
            QValue denominator = simpleAggregate(denominator_call, "sum");
            QValue result = QCompiler::scalar(fn("divide", numerator.ast, denominator.ast), numerator.depth);
            result.aggregated = numerator.aggregated || denominator.aggregated;
            result.qtype = QType::Float;
            return result;
        };
        h["wsum"] = [](Call & c)
        {
            c.expectArgs(2);
            QValue product = c.compiler.atomic(c.name, {c.scalarArg(0), c.scalarArg(1)},
                [](const ASTs & asts) { return fn("multiply", asts[0], asts[1]); }, c.scope);
            QValues sum_args{product};
            Call sum_call{c.compiler, c.name, sum_args, c.env, c.scope, c.offset};
            return simpleAggregate(sum_call, "sum");
        };
        auto pair_aggregate = [](const String & function)
        {
            return [function](Call & c)
            {
                c.expectArgs(2);
                QValue x = c.scalarArg(0);
                QValue y = c.scalarArg(1);
                if (x.kind == QValueKind::Column && y.kind == QValueKind::Column)
                {
                    QValue result = QCompiler::scalar(fn(function, c.compiler.columnForAggregation(x, c.scope), c.compiler.columnForAggregation(y, c.scope)));
                    result.aggregated = true;
                    result.qtype = QType::Float;
                    return result;
                }
                if (x.isList() && y.isList())
                {
                    QValue result = QCompiler::scalar(fn("arrayReduce", ASTs{lit(Field(function)), x.ast, y.ast}), x.depth - 1);
                    result.aggregated = x.aggregated || y.aggregated;
                    result.qtype = QType::Float;
                    return result;
                }
                c.fail(ErrorCodes::TYPE_MISMATCH, "expected two columns or two lists");
            };
        };
        h["cor"] = pair_aggregate("corr");
        h["cov"] = pair_aggregate("covarPop");
        h["scov"] = pair_aggregate("covarSamp");

        /// Lists.
        h["til"] = [](Call & c)
        {
            c.expectArgs(1);
            QValue result = c.compiler.atomic(c.name, {c.scalarArg(0)}, [](const ASTs & asts) { return fn("range", asts[0]); }, c.scope);
            result.depth += 1;
            result.qtype = QType::Long;
            return result;
        };
        h["distinct"] = [](Call & c)
        {
            c.expectArgs(1);
            if (c.arg(0).kind == QValueKind::Table)
            {
                QSelectBuilder builder = selectColumns(*c.arg(0).table);
                builder.distinct = true;
                return tableValue(c.compiler, builder.build());
            }
            return listFunction(c, [](ASTPtr list) { return fn("arrayDistinct", list); });
        };
        h["asc"] = [](Call & c) { return listFunction(c, [](ASTPtr list) { return fn("arraySort", list); }); };
        h["desc"] = [](Call & c) { return listFunction(c, [](ASTPtr list) { return fn("arrayReverseSort", list); }); };
        h["reverse"] = [](Call & c)
        {
            c.expectArgs(1);
            QValue value = c.scalarArg(0);
            if (value.is_string && value.depth == 0)
                return unary(c, "reverseUTF8", std::nullopt, true, true);
            return listFunction(c, [](ASTPtr list) { return fn("arrayReverse", list); });
        };
        auto grade = [](bool descending)
        {
            return [descending](Call & c)
            {
                return listFunction(c,
                    [&](ASTPtr list)
                    {
                        String index = c.compiler.newLambdaParameter();
                        String item = c.compiler.newLambdaParameter();
                        ASTPtr sorted = fn(descending ? "arrayReverseSort" : "arraySort",
                            makeLambda({index, item}, ident(item)), fn("arrayEnumerate", list), list);
                        String position = c.compiler.newLambdaParameter();
                        return fn("arrayMap", makeLambda({position}, fn("minus", ident(position), one())), sorted);
                    });
            };
        };
        h["iasc"] = grade(false);
        h["idesc"] = grade(true);
        h["where"] = [](Call & c) { return h_where(c); };
        h["enlist"] = [](Call & c)
        {
            QValues values;
            for (size_t i = 0; i < c.args.size(); ++i)
                values.push_back(c.scalarArg(i));
            if (values.empty())
                c.fail(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "expected an argument");
            QValue result = values[0];
            ASTs items;
            for (auto & value : values)
            {
                if (value.depth != values[0].depth || value.kind != values[0].kind)
                    c.unsupported("Lists of items of different shapes");
                result.aggregated |= value.aggregated;
                result.windowed |= value.windowed;
                items.push_back(value.ast);
            }
            result.ast = fn("array", items);
            result.depth += 1;
            result.is_string = values[0].is_string;
            return result;
        };
        h["raze"] = [](Call & c)
        {
            c.expectArgs(1);
            QValue value = c.scalarArg(0);
            if (value.kind == QValueKind::Column)
            {
                if (value.depth != 1)
                    c.unsupported("raze of a column of atoms");
                QValue result = QCompiler::scalar(fn("groupArrayArray", c.compiler.columnForAggregation(value, c.scope)), 1);
                result.aggregated = true;
                result.qtype = value.qtype;
                return result;
            }
            if (value.depth < 2)
                return value;
            if (value.depth > 2)
                c.unsupported("raze of lists nested deeper than two levels");
            QValue result = value;
            result.ast = fn("arrayFlatten", value.ast);
            result.depth = 1;
            return result;
        };

        /// Uniform functions.
        /// The running sums take a null for zero. The running maximum and minimum skip the nulls like the aggregates do.
        h["sums"] = [](Call & c)
        {
            c.expectArgs(1);
            return uniform(c,
                [&](ASTPtr column)
                {
                    return c.compiler.makeWindowFunction("sum", {fn("ifNull", std::move(column), lit(Field(static_cast<UInt64>(0))))}, c.scope, std::nullopt, 0);
                },
                [&](ASTPtr list) { return fn("arrayCumSum", nullsAsZeros(c.compiler, std::move(list))); });
        };
        auto running = [](const String & function)
        {
            return [function](Call & c)
            {
                c.expectArgs(1);
                return uniform(c,
                    [&](ASTPtr column) { return c.compiler.makeWindowFunction(function, {column}, c.scope, std::nullopt, 0); },
                    [&](ASTPtr list)
                    {
                        return reduceInRanges(c.compiler, function, list, [](ASTPtr index) { return fn("tuple", one(), std::move(index)); });
                    });
            };
        };
        h["maxs"] = running("max");
        h["mins"] = running("min");
        h["prds"] = [](Call & c)
        {
            c.expectArgs(1);
            return uniform(c,
                [&](ASTPtr column)
                {
                    return fn("arrayProduct", c.compiler.makeWindowFunction("groupArray", {column}, c.scope, std::nullopt, 0));
                },
                [&](ASTPtr list) { return runningProducts(c.compiler, std::move(list)); });
        };
        auto lag = [](Call & c, ASTPtr column, UInt64 n)
        {
            return c.compiler.makeWindowFunction("lagInFrame", {fn("toNullable", column), lit(Field(n))}, c.scope, n, 0);
        };
        h["prev"] = [lag](Call & c)
        {
            c.expectArgs(1);
            return uniform(c, [&](ASTPtr column) { return lag(c, column, 1); },
                [&](ASTPtr list) { return mapWithIndex(c.compiler, list, [&](ASTPtr, ASTPtr index) { return previousItem(list, index); }); });
        };
        h["next"] = [](Call & c)
        {
            c.expectArgs(1);
            return uniform(c,
                [&](ASTPtr column) { return c.compiler.makeWindowFunction("leadInFrame", {fn("toNullable", column), one()}, c.scope, 0, 1); },
                [&](ASTPtr list)
                {
                    return mapWithIndex(c.compiler, list,
                        [&](ASTPtr, ASTPtr index)
                        {
                            return fn("if", fn("equals", index, fn("length", list)), lit(Field()), fn("arrayElement", list, fn("plus", index, one())));
                        });
                });
        };
        h["xprev"] = [lag](Call & c)
        {
            c.expectArgs(2);
            auto n = constantInt(c.arg(0));
            if (!n || *n < 0)
                c.fail(ErrorCodes::BAD_ARGUMENTS, "the shift must be a non-negative integer literal");
            UInt64 shift = static_cast<UInt64>(*n);
            return uniform(c, [&](ASTPtr column) { return lag(c, column, shift); },
                [&](ASTPtr list)
                {
                    return mapWithIndex(c.compiler, list,
                        [&](ASTPtr, ASTPtr index)
                        {
                            return fn("if", fn("lessOrEquals", index, lit(Field(shift))), lit(Field()),
                                fn("arrayElement", list, fn("minus", index, lit(Field(shift)))));
                        });
                });
        };
        h["deltas"] = [lag](Call & c)
        {
            c.expectArgs(1);
            return uniform(c, [&](ASTPtr column) { return fn("minus", column, fn("ifNull", lag(c, column, 1), lit(Field(static_cast<UInt64>(0))))); },
                [&](ASTPtr list)
                {
                    return mapWithIndex(c.compiler, list,
                        [&](ASTPtr item, ASTPtr index) { return fn("minus", item, fn("ifNull", previousItem(list, index), lit(Field(static_cast<UInt64>(0))))); });
                });
        };
        h["ratios"] = [lag](Call & c)
        {
            c.expectArgs(1);
            return uniform(c, [&](ASTPtr column) { return fn("divide", column, fn("ifNull", lag(c, column, 1), one())); },
                [&](ASTPtr list)
                {
                    return mapWithIndex(c.compiler, list,
                        [&](ASTPtr item, ASTPtr index) { return fn("divide", item, fn("ifNull", previousItem(list, index), one())); });
                });
        };
        h["differ"] = [lag](Call & c)
        {
            c.expectArgs(1);
            /// The first item always differs, and the others are compared with their predecessors by the rules of q:
            /// a null does not differ from a null.
            QValue result = uniform(c,
                [&](ASTPtr column)
                {
                    ASTPtr position = c.compiler.makeWindowFunction("count", {}, c.scope, std::nullopt, 0);
                    return fn("if", fn("equals", std::move(position), one()), one(), compare("notEquals", column, lag(c, column, 1), false));
                },
                [&](ASTPtr list)
                {
                    return mapWithIndex(c.compiler, list,
                        [&](ASTPtr item, ASTPtr index)
                        {
                            return fn("if", fn("equals", index, one()), one(), compare("notEquals", item, previousItem(list, index), false));
                        });
                });
            result.qtype = QType::Boolean;
            return result;
        };
        h["fills"] = [](Call & c)
        {
            c.expectArgs(1);
            return uniform(c, [&](ASTPtr column) { return c.compiler.makeWindowFunction("anyLast", {column}, c.scope, std::nullopt, 0); },
                [&](ASTPtr list)
                {
                    String item = c.compiler.newLambdaParameter();
                    return fn("arrayFill", makeLambda({item}, fn("isNotNull", ident(item))), list);
                });
        };
        h["mavg"] = [](Call & c) { return movingWindow(c, "avg"); };
        h["msum"] = [](Call & c) { return movingWindow(c, "sum"); };
        h["mmax"] = [](Call & c) { return movingWindow(c, "max"); };
        h["mmin"] = [](Call & c) { return movingWindow(c, "min"); };
        h["mcount"] = [](Call & c) { return movingWindow(c, "count"); };
        h["mdev"] = [](Call & c)
        {
            return movingWindow(c, "stddevPop");
        };

        /// Adverbs as keywords.
        auto adverb = [](const String & name)
        {
            return [name](Call & c)
            {
                c.expectArgs(2);
                if (c.arg(0).kind != QValueKind::Function)
                    c.fail(ErrorCodes::TYPE_MISMATCH, "the left argument must be a function");
                QFunction derived;
                derived.adverb = name;
                derived.operand = c.arg(0).function;
                return c.compiler.applyDerived(derived, {c.arg(1)}, c.env, c.scope, c.offset);
            };
        };
        h["each"] = adverb("'");
        h["peach"] = adverb("'");
        h["over"] = adverb("/");
        h["scan"] = adverb("\\");
        h["prior"] = adverb("':");

        /// Tables.
        h["cols"] = [](Call & c)
        {
            c.expectArgs(1);
            const QTable & table = c.tableArg(0);
            ASTs names;
            for (const auto & column : table.columns)
                names.push_back(lit(Field(column.name)));
            QValue result = QCompiler::scalar(fn("array", names), 1);
            result.qtype = QType::Symbol;
            return result;
        };
        h["meta"] = [](Call & c)
        {
            c.expectArgs(1);
            return metaTable(c, c.tableArg(0));
        };
        h["keys"] = [](Call & c)
        {
            c.expectArgs(1);
            const QTable & table = c.tableArg(0);
            ASTs names;
            for (const auto & key : table.keys)
                names.push_back(lit(Field(key)));
            QValue result = QCompiler::scalar(fn("array", names), 1);
            result.qtype = QType::Symbol;
            return result;
        };
        h["key"] = [](Call & c)
        {
            c.expectArgs(1);
            if (c.arg(0).kind == QValueKind::Dictionary)
                return c.arg(0).dictionary->first;
            const QTable & table = c.tableArg(0);
            if (table.keys.empty())
                c.fail(ErrorCodes::TYPE_MISMATCH, "the table is not keyed");
            QSelectBuilder builder;
            builder.from = table.table_expression;
            for (const auto & key : table.keys)
                builder.select.push_back(ident(key));
            return tableValue(c.compiler, builder.build());
        };
        h["value"] = [](Call & c)
        {
            c.expectArgs(1);
            if (c.arg(0).kind == QValueKind::Dictionary)
                return c.arg(0).dictionary->second;
            const QTable & table = c.tableArg(0);
            std::unordered_set<String> keys(table.keys.begin(), table.keys.end());
            QSelectBuilder builder;
            builder.from = table.table_expression;
            for (const auto & column : table.columns)
                if (!keys.contains(column.name))
                    builder.select.push_back(ident(column.name));
            return tableValue(c.compiler, builder.build());
        };
        h["flip"] = [](Call & c)
        {
            c.expectArgs(1);
            if (c.arg(0).kind != QValueKind::Dictionary)
                c.unsupported("flip of a value that is not a dictionary of symbols to lists");
            const auto & [keys, values] = *c.arg(0).dictionary;
            auto names = constantSymbols(keys);
            const auto * array = values.ast->as<ASTFunction>();
            if (!names || !array || array->name != "array" || array->arguments->children.size() != names->size())
                c.unsupported("flip of a dictionary that is not a literal");
            QSelectBuilder inner;
            QSelectBuilder outer;
            for (size_t i = 0; i < names->size(); ++i)
            {
                inner.select.push_back(withAlias(array->arguments->children[i], (*names)[i]));
                outer.select.push_back(ident((*names)[i]));
                if (values.depth > 1)
                    outer.array_join.push_back(ident((*names)[i]));
            }
            if (outer.array_join.empty())
                return tableValue(c.compiler, inner.build());
            outer.from = makeSubqueryTableExpression(inner.build());
            return tableValue(c.compiler, outer.build());
        };
        h["xasc"] = [](Call & c)
        {
            c.expectArgs(2);
            auto columns = constantSymbols(c.arg(0));
            if (!columns)
                c.fail(ErrorCodes::TYPE_MISMATCH, "the left argument must be the symbols of the columns");
            return sortTable(c, *columns, c.tableArg(1), false);
        };
        h["xdesc"] = [](Call & c)
        {
            c.expectArgs(2);
            auto columns = constantSymbols(c.arg(0));
            if (!columns)
                c.fail(ErrorCodes::TYPE_MISMATCH, "the left argument must be the symbols of the columns");
            return sortTable(c, *columns, c.tableArg(1), true);
        };
        h["xkey"] = [](Call & c)
        {
            c.expectArgs(2);
            const QTable & table = c.tableArg(1);
            Strings keys;
            if (const auto * array = c.arg(0).ast ? c.arg(0).ast->as<ASTFunction>() : nullptr; array && array->name == "array" && array->arguments->children.empty())
                keys = {};
            else if (auto columns = constantSymbols(c.arg(0)))
                keys = *columns;
            else
                c.fail(ErrorCodes::TYPE_MISMATCH, "the left argument must be the symbols of the key columns");
            return keyTable(c, keys, table);
        };
        h["xcol"] = [](Call & c)
        {
            c.expectArgs(2);
            auto names = constantSymbols(c.arg(0));
            if (!names)
                c.fail(ErrorCodes::TYPE_MISMATCH, "the left argument must be the symbols of the new names");
            const QTable & table = c.tableArg(1);
            if (names->size() > table.columns.size())
                c.fail(ErrorCodes::BAD_ARGUMENTS, "too many names");
            QSelectBuilder builder;
            builder.from = table.table_expression;
            Names keys;
            QTypes qtypes;
            size_t i = 0;
            for (const auto & column : table.columns)
            {
                String name = i < names->size() ? (*names)[i] : column.name;
                builder.select.push_back(withAlias(ident(column.name), name));
                if (std::find(table.keys.begin(), table.keys.end(), column.name) != table.keys.end())
                    keys.push_back(name);
                if (auto it = table.qtypes.find(column.name); it != table.qtypes.end())
                    qtypes[name] = it->second;
                ++i;
            }
            c.renames_columns = true;
            return QCompiler::withQTypes(tableValue(c.compiler, builder.build(), keys), qtypes);
        };
        h["xcols"] = [](Call & c)
        {
            c.expectArgs(2);
            auto names = constantSymbols(c.arg(0));
            if (!names)
                c.fail(ErrorCodes::TYPE_MISMATCH, "the left argument must be the symbols of the columns");
            const QTable & table = c.tableArg(1);
            QSelectBuilder builder;
            builder.from = table.table_expression;
            std::unordered_set<String> first(names->begin(), names->end());
            for (const auto & name : *names)
            {
                if (!hasColumn(table, name))
                    c.fail(ErrorCodes::UNKNOWN_IDENTIFIER, fmt::format("unknown column '{}'", name));
                builder.select.push_back(ident(name));
            }
            for (const auto & column : table.columns)
                if (!first.contains(column.name))
                    builder.select.push_back(ident(column.name));
            return tableValue(c.compiler, builder.build());
        };
        h["lj"] = [](Call & c)
        {
            c.expectArgs(2);
            return joinTables(c, QJoinKind::Left, c.tableArg(0), c.tableArg(1), c.tableArg(1).keys);
        };
        h["ljf"] = h["lj"];
        h["ij"] = [](Call & c)
        {
            c.expectArgs(2);
            return joinTables(c, QJoinKind::Inner, c.tableArg(0), c.tableArg(1), c.tableArg(1).keys);
        };
        h["ijf"] = h["ij"];
        h["ej"] = [](Call & c)
        {
            c.expectArgs(3);
            auto keys = constantSymbols(c.arg(0));
            if (!keys)
                c.fail(ErrorCodes::TYPE_MISMATCH, "the first argument must be the symbols of the key columns");
            return joinTables(c, QJoinKind::Equi, c.tableArg(1), c.tableArg(2), Names(keys->begin(), keys->end()));
        };
        h["aj"] = [](Call & c) { return asOfJoin(c, false); };
        h["aj0"] = [](Call & c) { return asOfJoin(c, true); };
        h["uj"] = [](Call & c)
        {
            c.expectArgs(2);
            return unionJoin(c, c.tableArg(0), c.tableArg(1));
        };
        h["ujf"] = h["uj"];
        h["cross"] = [](Call & c)
        {
            c.expectArgs(2);
            if (c.arg(0).kind != QValueKind::Table || c.arg(1).kind != QValueKind::Table)
            {
                QValue left = c.scalarArg(0);
                QValue right = c.scalarArg(1);
                if (!left.isList() || !right.isList())
                    c.unsupported("cross of values that are not lists");
                String a = c.compiler.newLambdaParameter();
                String b = c.compiler.newLambdaParameter();
                /// A list of the pairs for each item of the left list, and these lists joined together. Only this outer
                /// layer is flattened: `arrayFlatten` would dissolve the pairs as well.
                ASTPtr pairs = fn("arrayReduce", lit(Field(String("groupArrayArray"))),
                    fn("arrayMap", makeLambda({a}, fn("arrayMap", makeLambda({b}, fn("array", ident(a), ident(b))), right.ast)), left.ast));
                QValue result = QCompiler::scalar(pairs, left.depth + 1);
                result.qtype = left.qtype;
                return result;
            }
            const QTable & left = c.tableArg(0);
            const QTable & right = c.tableArg(1);
            /// The result is in the order of the rows of the left table, and then of the right one,
            /// while a `CROSS JOIN` gives no order (the tables can be swapped), so the rows are numbered.
            QSelectBuilder left_numbered = selectColumns(left);
            left_numbered.select.push_back(withAlias(fn("rowNumberInAllBlocks", ASTs{}), "__qrow"));
            QSelectBuilder right_numbered = selectColumns(right);
            right_numbered.select.push_back(withAlias(fn("rowNumberInAllBlocks", ASTs{}), "__qrow"));
            QSelectBuilder builder;
            builder.from = makeSubqueryTableExpression(left_numbered.build(), "__ql");
            auto table_join = make_intrusive<ASTTableJoin>();
            table_join->kind = JoinKind::Cross;
            builder.joins.emplace_back(makeSubqueryTableExpression(right_numbered.build(), "__qr"), table_join);
            builder.order_by.emplace_back(qualified("__ql", "__qrow"), false);
            builder.order_by.emplace_back(qualified("__qr", "__qrow"), false);
            for (const auto & column : left.columns)
                builder.select.push_back(withAlias(qualified("__ql", column.name), column.name));
            for (const auto & column : right.columns)
            {
                if (hasColumn(left, column.name))
                    c.fail(ErrorCodes::BAD_ARGUMENTS, fmt::format("both tables have the column '{}'", column.name));
                builder.select.push_back(withAlias(qualified("__qr", column.name), column.name));
            }
            return tableValue(c.compiler, builder.build());
        };
        h["sublist"] = [](Call & c)
        {
            c.expectArgs(2);
            QValue range = c.scalarArg(0);
            QValue list = c.scalarArg(1);
            if (list.kind == QValueKind::Table || list.depth == 0)
                c.unsupported("sublist of a value that is not a list");
            QValue result = list;
            if (range.depth == 1)
                result.ast = fn("arraySlice", list.ast, fn("plus", fn("arrayElement", range.ast, one()), one()),
                    fn("arrayElement", range.ast, lit(Field(static_cast<UInt64>(2)))));
            else if (auto n = constantInt(range); n && *n < 0)
                result.ast = fn("arraySlice", list.ast, lit(Field(*n)));
            else
                result.ast = fn("arraySlice", list.ast, one(), range.ast);
            return result;
        };
        h[".ch.sql"] = [](Call & c) { return sqlTable(c); };

        for (const char * name : {"insert", "upsert", "set", "group", "type", "value_unsupported", "ss", "rank", "ema", "cut", "rotate",
                 "lsq", "mmu", "inv", "xgroup", "xrank", "pj", "wj", "wj1", "binr", "dsave", "fkeys", "tables", "views", "gtime", "ltime",
                 "parse", "eval", "value_of", "get", "hopen", "hclose", "read0", "read1", "exit", "show", "system"})
        {
            String keyword = name;
            if (!h.contains(keyword))
                h[keyword] = [](Call & c) -> QValue { c.unsupported(fmt::format("The function '{}'", c.name)); };
        }
        return h;
    }();
    return map;
}

}

bool QCompiler::isBuiltin(const String & name)
{
    return handlers().contains(name);
}

size_t QCompiler::builtinArity(const String & name)
{
    static const std::unordered_set<String> binary = {
        "and", "or", "mod", "div", "xbar", "xexp", "xlog", "in", "within", "like", "sv", "vs", "bin", "except", "inter", "union",
        "wavg", "wsum", "cor", "cov", "scov", "xprev", "mavg", "msum", "mmax", "mmin", "mcount", "mdev", "each", "peach", "over",
        "scan", "prior", "xasc", "xdesc", "xkey", "xcol", "xcols", "lj", "ljf", "ij", "ijf", "uj", "ujf", "cross", "sublist",
    };
    static const std::unordered_set<String> ternary = {"aj", "aj0", "ej", "ssr"};
    if (binary.contains(name))
        return 2;
    if (ternary.contains(name))
        return 3;
    if (name.empty() || !isAlphaASCII(name[0]))
        return 0;
    return 1;
}

QValue QCompiler::callBuiltin(const String & name, QValues args, QEnvironment & env, QScope * scope, size_t offset)
{
    Call call{*this, name, args, env, scope, offset};

    /// Temporal accessors of columns: `time.hh`.
    if (!name.empty() && name[0] == '.' && !handlers().contains(name))
    {
        auto access = accessor(name.substr(1));
        if (!access)
            throwQError(ErrorCodes::UNKNOWN_IDENTIFIER, offset, fmt::format("Unknown field '{}' of a column", name.substr(1)));
        QValue result = atomic(name, {call.scalarArg(0)}, [&](const ASTs & asts) { return access->first(asts[0]); }, scope);
        result.qtype = access->second;
        result.is_string = false;
        return result;
    }

    auto it = handlers().find(name);
    if (it == handlers().end())
        throwQError(ErrorCodes::UNKNOWN_IDENTIFIER, offset, fmt::format("Unknown function '{}'", name));
    QValue result = it->second(call);

    /// A table that a function makes of its argument tables (`n#t`, a join, a sort) keeps the q types of their columns
    /// that the types of ClickHouse do not tell: the columns go to the result under their names. If the argument tables
    /// do not agree about a name (a month in one of them and a date in another), it is not known whose column the result
    /// has, and the type of ClickHouse tells its q type.
    if (result.kind == QValueKind::Table && !call.renames_columns)
    {
        std::map<String, std::optional<QType>> seen;
        std::set<String> disagreed;
        for (const auto & arg : args)
        {
            if (arg.kind != QValueKind::Table || !arg.table)
                continue;
            for (const auto & column : arg.table->columns)
            {
                std::optional<QType> hint;
                if (auto found = arg.table->qtypes.find(column.name); found != arg.table->qtypes.end())
                    hint = found->second;
                auto [position, inserted] = seen.emplace(column.name, hint);
                if (!inserted && position->second != hint)
                    disagreed.insert(column.name);
            }
        }

        QTypes inherited;
        for (const auto & [column_name, hint] : seen)
            if (hint && !disagreed.contains(column_name))
                inherited.emplace(column_name, *hint);
        if (!inherited.empty())
            result = withQTypes(result, inherited);
    }
    return result;
}

}

}
