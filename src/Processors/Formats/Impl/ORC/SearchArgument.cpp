#include <Processors/Formats/Impl/ORC/SearchArgument.h>

#include <Common/Exception.h>
#include <IO/WriteHelpers.h>
#include <base/extended_types.h>

#include <bit>
#include <cmath>
#include <deque>
#include <unordered_set>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_DATA;
}
}

namespace DB::ORC
{

TruthValue operator||(TruthValue left, TruthValue right)
{
    if (right == TruthValue::YES || left == TruthValue::YES)
        return TruthValue::YES;
    if (right == TruthValue::YES_NULL || left == TruthValue::YES_NULL)
        return TruthValue::YES_NULL;
    if (right == TruthValue::NO)
        return left;
    if (left == TruthValue::NO)
        return right;
    if (left == TruthValue::IS_NULL)
    {
        if (right == TruthValue::NO_NULL || right == TruthValue::IS_NULL)
            return TruthValue::IS_NULL;
        return TruthValue::YES_NULL;
    }
    if (right == TruthValue::IS_NULL)
    {
        if (left == TruthValue::NO_NULL)
            return TruthValue::IS_NULL;
        return TruthValue::YES_NULL;
    }
    if (left == TruthValue::NO_NULL && right == TruthValue::NO_NULL)
        return TruthValue::NO_NULL;
    return TruthValue::YES_NO_NULL;
}

TruthValue operator&&(TruthValue left, TruthValue right)
{
    if (right == TruthValue::NO || left == TruthValue::NO)
        return TruthValue::NO;
    if (right == TruthValue::NO_NULL || left == TruthValue::NO_NULL)
        return TruthValue::NO_NULL;
    if (right == TruthValue::YES)
        return left;
    if (left == TruthValue::YES)
        return right;
    if (left == TruthValue::IS_NULL)
    {
        if (right == TruthValue::YES_NULL || right == TruthValue::IS_NULL)
            return TruthValue::IS_NULL;
        return TruthValue::NO_NULL;
    }
    if (right == TruthValue::IS_NULL)
    {
        if (left == TruthValue::YES_NULL)
            return TruthValue::IS_NULL;
        return TruthValue::NO_NULL;
    }
    if (left == TruthValue::YES_NULL && right == TruthValue::YES_NULL)
        return TruthValue::YES_NULL;
    return TruthValue::YES_NO_NULL;
}

TruthValue operator!(TruthValue value)
{
    switch (value)
    {
        case TruthValue::NO: return TruthValue::YES;
        case TruthValue::YES: return TruthValue::NO;
        case TruthValue::IS_NULL:
        case TruthValue::YES_NO:
        case TruthValue::YES_NO_NULL:
            return value;
        case TruthValue::NO_NULL: return TruthValue::YES_NULL;
        case TruthValue::YES_NULL: return TruthValue::NO_NULL;
    }
    return value;
}

bool isNeeded(TruthValue value)
{
    switch (value)
    {
        case TruthValue::NO:
        case TruthValue::IS_NULL:
        case TruthValue::NO_NULL:
            return false;
        default:
            return true;
    }
}


bool Literal::Decimal::parse(std::string_view s)
{
    bool negative = false;
    size_t i = 0;
    if (i < s.size() && (s[i] == '-' || s[i] == '+'))
    {
        negative = s[i] == '-';
        ++i;
    }
    Int256 result = 0;
    bool has_digits = false;
    bool has_point = false;
    Int32 result_scale = 0;
    for (; i < s.size(); ++i)
    {
        char c = s[i];
        if (c == '.' && !has_point)
        {
            has_point = true;
            continue;
        }
        if (c < '0' || c > '9')
            return false;
        has_digits = true;
        result = result * 10 + (c - '0');
        if (has_point)
            ++result_scale;
        /// More than 38 digits do not fit into the 128-bit value.
        if (result > Int256(std::numeric_limits<Int128>::max()))
            return false;
    }
    if (!has_digits)
        return false;
    value = static_cast<Int128>(negative ? -result : result);
    scale = result_scale;
    return true;
}

std::string Literal::Decimal::toString() const
{
    bool negative = value < 0;
    std::string digits = DB::toString(negative ? -Int256(value) : Int256(value));
    std::string result;
    if (scale <= 0)
        result = digits;
    else
    {
        if (digits.size() <= static_cast<size_t>(scale))
            digits = std::string(scale - digits.size() + 1, '0') + digits;
        result = digits.substr(0, digits.size() - scale) + "." + digits.substr(digits.size() - scale);
        size_t last = result.find_last_not_of('0');
        if (result[last] == '.')
            result.resize(last);
        else
            result.resize(last + 1);
    }
    return negative ? "-" + result : result;
}

namespace
{

int compareDecimals(const Literal::Decimal & a, const Literal::Decimal & b)
{
    Int256 left = a.value;
    Int256 right = b.value;
    for (Int32 s = a.scale; s < b.scale; ++s)
        left *= 10;
    for (Int32 s = b.scale; s < a.scale; ++s)
        right *= 10;
    return left < right ? -1 : (left == right ? 0 : 1);
}

}

Literal::Literal(bool value) : type(PredicateDataType::BOOLEAN), int_value(value) {}
Literal::Literal(Int64 value) : type(PredicateDataType::LONG), int_value(value) {}
Literal::Literal(double value) : type(PredicateDataType::FLOAT), float_value(value) {}
Literal::Literal(const char * data, size_t size) : type(PredicateDataType::STRING), string_value(data, size) {}

Literal::Literal(PredicateDataType type_, Int64 value) : type(type_), int_value(value)
{
    if (type != PredicateDataType::DATE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Only DATE literals can be created from a type and an integer");
}

Literal::Literal(Int64 second, Int32 nanos) : type(PredicateDataType::TIMESTAMP), timestamp_value{second, nanos} {}

Literal::Literal(Int128 value, Int32 /*precision*/, Int32 scale) : type(PredicateDataType::DECIMAL), decimal_value{value, scale} {}


namespace
{

/// Where a point is relative to the range [min, max].
enum class Location : UInt8
{
    BEFORE,
    MIN,
    MIDDLE,
    MAX,
    AFTER,
};

template <typename T, typename Less, typename Equal>
Location compareToRange(const T & point, const T & min, const T & max, Less less, Equal equal)
{
    if (less(point, min))
        return Location::BEFORE;
    if (equal(point, min))
        return Location::MIN;
    if (less(max, point))
        return Location::AFTER;
    if (equal(point, max))
        return Location::MAX;
    return Location::MIDDLE;
}

template <typename T, typename Less, typename Equal>
TruthValue evaluatePredicateRange(
    PredicateLeaf::Operator op, const std::vector<T> & values, const T & min_value, const T & max_value, bool has_null, Less less, Equal equal)
{
    auto loc_of = [&](const T & v) { return compareToRange(v, min_value, max_value, less, equal); };
    bool min_equals_max = equal(min_value, max_value);

    switch (op)
    {
        case PredicateLeaf::Operator::NULL_SAFE_EQUALS:
        {
            auto loc = loc_of(values.at(0));
            if (loc == Location::BEFORE || loc == Location::AFTER)
                return TruthValue::NO;
            return TruthValue::YES_NO;
        }
        case PredicateLeaf::Operator::EQUALS:
        {
            auto loc = loc_of(values.at(0));
            if (min_equals_max && loc == Location::MIN)
                return has_null ? TruthValue::YES_NULL : TruthValue::YES;
            if (loc == Location::BEFORE || loc == Location::AFTER)
                return has_null ? TruthValue::NO_NULL : TruthValue::NO;
            return has_null ? TruthValue::YES_NO_NULL : TruthValue::YES_NO;
        }
        case PredicateLeaf::Operator::LESS_THAN:
        {
            auto loc = loc_of(values.at(0));
            if (loc == Location::AFTER)
                return has_null ? TruthValue::YES_NULL : TruthValue::YES;
            if (loc == Location::BEFORE || loc == Location::MIN)
                return has_null ? TruthValue::NO_NULL : TruthValue::NO;
            return has_null ? TruthValue::YES_NO_NULL : TruthValue::YES_NO;
        }
        case PredicateLeaf::Operator::LESS_THAN_EQUALS:
        {
            auto loc = loc_of(values.at(0));
            if (loc == Location::AFTER || loc == Location::MAX || (loc == Location::MIN && min_equals_max))
                return has_null ? TruthValue::YES_NULL : TruthValue::YES;
            if (loc == Location::BEFORE)
                return has_null ? TruthValue::NO_NULL : TruthValue::NO;
            return has_null ? TruthValue::YES_NO_NULL : TruthValue::YES_NO;
        }
        case PredicateLeaf::Operator::IN:
        {
            if (min_equals_max)
            {
                for (const auto & value : values)
                    if (loc_of(value) == Location::MIN)
                        return has_null ? TruthValue::YES_NULL : TruthValue::YES;
                return has_null ? TruthValue::NO_NULL : TruthValue::NO;
            }
            for (const auto & value : values)
            {
                auto loc = loc_of(value);
                if (loc == Location::MIN || loc == Location::MIDDLE || loc == Location::MAX)
                    return has_null ? TruthValue::YES_NO_NULL : TruthValue::YES_NO;
            }
            return has_null ? TruthValue::NO_NULL : TruthValue::NO;
        }
        case PredicateLeaf::Operator::BETWEEN:
        {
            if (values.empty())
                return TruthValue::YES_NO;
            auto loc = loc_of(values.at(0));
            if (loc == Location::BEFORE || loc == Location::MIN)
            {
                auto loc2 = loc_of(values.at(1));
                if (loc2 == Location::AFTER || loc2 == Location::MAX)
                    return has_null ? TruthValue::YES_NULL : TruthValue::YES;
                if (loc2 == Location::BEFORE)
                    return has_null ? TruthValue::NO_NULL : TruthValue::NO;
                return has_null ? TruthValue::YES_NO_NULL : TruthValue::YES_NO;
            }
            if (loc == Location::AFTER)
                return has_null ? TruthValue::NO_NULL : TruthValue::NO;
            return has_null ? TruthValue::YES_NO_NULL : TruthValue::YES_NO;
        }
        case PredicateLeaf::Operator::IS_NULL:
            return has_null ? TruthValue::YES_NO : TruthValue::NO;
    }
    return has_null ? TruthValue::YES_NO_NULL : TruthValue::YES_NO;
}

template <typename T>
TruthValue evaluateRange(PredicateLeaf::Operator op, const std::vector<T> & values, const T & min_value, const T & max_value, bool has_null)
{
    return evaluatePredicateRange(op, values, min_value, max_value, has_null,
        [](const T & a, const T & b) { return a < b; },
        [](const T & a, const T & b) { return a == b; });
}

TruthValue evaluateBoolPredicate(PredicateLeaf::Operator op, const std::vector<Literal> & literals, const Proto::ColumnStatistics & stats)
{
    bool has_null = stats.has_null.value_or(false);
    if (!stats.bucket_statistics || stats.bucket_statistics->count.empty())
        return has_null ? TruthValue::YES_NO_NULL : TruthValue::YES_NO;

    UInt64 true_count = stats.bucket_statistics->count[0];
    UInt64 false_count = stats.number_of_values.value_or(0) - true_count;
    switch (op)
    {
        case PredicateLeaf::Operator::IS_NULL:
            return has_null ? TruthValue::YES_NO : TruthValue::NO;
        case PredicateLeaf::Operator::NULL_SAFE_EQUALS:
        {
            if (literals.at(0).getBool())
            {
                if (true_count == 0)
                    return TruthValue::NO;
                if (false_count == 0)
                    return TruthValue::YES;
            }
            else
            {
                if (false_count == 0)
                    return TruthValue::NO;
                if (true_count == 0)
                    return TruthValue::YES;
            }
            return TruthValue::YES_NO;
        }
        case PredicateLeaf::Operator::EQUALS:
        {
            if (literals.at(0).getBool())
            {
                if (true_count == 0)
                    return has_null ? TruthValue::NO_NULL : TruthValue::NO;
                if (false_count == 0)
                    return has_null ? TruthValue::YES_NULL : TruthValue::YES;
            }
            else
            {
                if (false_count == 0)
                    return has_null ? TruthValue::NO_NULL : TruthValue::NO;
                if (true_count == 0)
                    return has_null ? TruthValue::YES_NULL : TruthValue::YES;
            }
            return has_null ? TruthValue::YES_NO_NULL : TruthValue::YES_NO;
        }
        default:
            return has_null ? TruthValue::YES_NO_NULL : TruthValue::YES_NO;
    }
}

template <typename T, typename Get>
std::vector<T> literalsTo(const std::vector<Literal> & literals, Get get)
{
    std::vector<T> result;
    for (const auto & literal : literals)
        if (!literal.isNull())
            result.push_back(get(literal));
    return result;
}

/// From the reference implementation: the writer version that fixed the timestamp statistics.
constexpr UInt32 WRITER_VERSION_ORC_135 = 6;

}

TruthValue PredicateLeaf::evaluatePredicateMinMax(const Proto::ColumnStatistics & stats) const
{
    TruthValue result = TruthValue::YES_NO_NULL;
    bool has_null = stats.has_null.value_or(false);

    switch (type)
    {
        case PredicateDataType::LONG:
        {
            if (stats.int_statistics && stats.int_statistics->minimum && stats.int_statistics->maximum)
                result = evaluateRange(op, literalsTo<Int64>(literals, [](const Literal & l) { return l.getLong(); }),
                    *stats.int_statistics->minimum, *stats.int_statistics->maximum, has_null);
            break;
        }
        case PredicateDataType::FLOAT:
        {
            if (stats.double_statistics && stats.double_statistics->minimum && stats.double_statistics->maximum)
            {
                if (!std::isfinite(stats.double_statistics->sum.value_or(0)))
                    result = has_null ? TruthValue::YES_NO_NULL : TruthValue::YES_NO;
                else
                    result = evaluateRange(op, literalsTo<double>(literals, [](const Literal & l) { return l.getFloat(); }),
                        *stats.double_statistics->minimum, *stats.double_statistics->maximum, has_null);
            }
            break;
        }
        case PredicateDataType::STRING:
        {
            if (stats.string_statistics && stats.string_statistics->minimum && stats.string_statistics->maximum)
                result = evaluateRange(op, literalsTo<std::string>(literals, [](const Literal & l) { return l.getString(); }),
                    *stats.string_statistics->minimum, *stats.string_statistics->maximum, has_null);
            break;
        }
        case PredicateDataType::DATE:
        {
            if (stats.date_statistics && stats.date_statistics->minimum && stats.date_statistics->maximum)
                result = evaluateRange(op, literalsTo<Int64>(literals, [](const Literal & l) { return l.getDate(); }),
                    static_cast<Int64>(*stats.date_statistics->minimum), static_cast<Int64>(*stats.date_statistics->maximum), has_null);
            break;
        }
        case PredicateDataType::TIMESTAMP:
        {
            const auto & ts = stats.timestamp_statistics;
            if (ts && ts->minimum_utc && ts->maximum_utc)
            {
                Int32 min_nanos = ts->minimum_nanos ? *ts->minimum_nanos - 1 : 0;
                Int32 max_nanos = ts->maximum_nanos ? *ts->maximum_nanos - 1 : 999999;
                Literal::Timestamp min{*ts->minimum_utc / 1000, static_cast<Int32>((*ts->minimum_utc % 1000) * 1000000) + min_nanos};
                Literal::Timestamp max{*ts->maximum_utc / 1000, static_cast<Int32>((*ts->maximum_utc % 1000) * 1000000) + max_nanos};
                result = evaluateRange(op, literalsTo<Literal::Timestamp>(literals, [](const Literal & l) { return l.getTimestamp(); }),
                    min, max, has_null);
            }
            break;
        }
        case PredicateDataType::DECIMAL:
        {
            const auto & ds = stats.decimal_statistics;
            if (ds && ds->minimum && ds->maximum)
            {
                Literal::Decimal min;
                Literal::Decimal max;
                if (min.parse(*ds->minimum) && max.parse(*ds->maximum))
                    result = evaluatePredicateRange(op,
                        literalsTo<Literal::Decimal>(literals, [](const Literal & l) { return l.getDecimal(); }),
                        min, max, has_null,
                        [](const Literal::Decimal & a, const Literal::Decimal & b) { return compareDecimals(a, b) < 0; },
                        [](const Literal::Decimal & a, const Literal::Decimal & b) { return compareDecimals(a, b) == 0; });
            }
            break;
        }
        case PredicateDataType::BOOLEAN:
        {
            if (stats.bucket_statistics)
                result = evaluateBoolPredicate(op, literals, stats);
            break;
        }
    }

    /// Respect a null literal for IN.
    if (op == Operator::IN && has_null)
        for (const auto & literal : literals)
            if (literal.isNull())
                return TruthValue::YES_NO_NULL;

    return result;
}

namespace
{

TruthValue checkInBloomFilter(PredicateDataType type, const Literal & literal, const BloomFilter & bf, bool has_null)
{
    TruthValue result = has_null ? TruthValue::NO_NULL : TruthValue::NO;
    if (literal.isNull())
        result = has_null ? TruthValue::YES_NO_NULL : TruthValue::NO;
    else
    {
        bool found = true;
        switch (type)
        {
            case PredicateDataType::LONG: found = bf.testLong(literal.getLong()); break;
            case PredicateDataType::FLOAT: found = bf.testDouble(literal.getFloat()); break;
            case PredicateDataType::STRING: found = bf.testBytes(literal.getString().data(), literal.getString().size()); break;
            case PredicateDataType::DECIMAL:
            {
                std::string s = literal.getDecimal().toString();
                found = bf.testBytes(s.data(), s.size());
                break;
            }
            case PredicateDataType::TIMESTAMP: found = bf.testLong(literal.getTimestamp().getMillis()); break;
            case PredicateDataType::DATE: found = bf.testLong(literal.getDate()); break;
            case PredicateDataType::BOOLEAN: found = true; break;
        }
        if (found)
            result = TruthValue::YES_NO_NULL;
    }

    if (result == TruthValue::YES_NO_NULL && !has_null)
        result = TruthValue::YES_NO;
    return result;
}

}

TruthValue PredicateLeaf::evaluatePredicateBloomFilter(const BloomFilter & bloom_filter, bool has_null) const
{
    switch (op)
    {
        case Operator::NULL_SAFE_EQUALS:
            /// Null-safe equals does not return the `*_NULL` variants.
            return checkInBloomFilter(type, literals.front(), bloom_filter, false);
        case Operator::EQUALS:
            return checkInBloomFilter(type, literals.front(), bloom_filter, has_null);
        case Operator::IN:
            for (const auto & literal : literals)
            {
                TruthValue result = checkInBloomFilter(type, literal, bloom_filter, has_null);
                if (result == TruthValue::YES_NO_NULL || result == TruthValue::YES_NO)
                    return result;
            }
            return has_null ? TruthValue::NO_NULL : TruthValue::NO;
        default:
            return has_null ? TruthValue::YES_NO_NULL : TruthValue::YES_NO;
    }
}

TruthValue PredicateLeaf::evaluate(UInt32 writer_version, const Proto::ColumnStatistics & stats, const BloomFilter * bloom_filter) const
{
    /// The files written before ORC-135 store the timestamps in the local time zone of the writer.
    if (type == PredicateDataType::TIMESTAMP && writer_version < WRITER_VERSION_ORC_135)
        return TruthValue::YES_NO_NULL;

    /// The files written by some writers do not have the `has_null` field.
    if (!stats.has_null)
        return TruthValue::YES_NO_NULL;

    bool has_null = *stats.has_null;
    bool all_null = has_null && stats.number_of_values.value_or(0) == 0;
    if (op == Operator::IS_NULL || ((op == Operator::EQUALS || op == Operator::NULL_SAFE_EQUALS) && literals.at(0).isNull()))
        return all_null ? TruthValue::YES : (has_null ? TruthValue::YES_NO : TruthValue::NO);
    if (all_null)
        return TruthValue::IS_NULL;

    TruthValue result = evaluatePredicateMinMax(stats);
    if (bloom_filter && result != TruthValue::NO_NULL && result != TruthValue::NO
        && (op == Operator::EQUALS || op == Operator::NULL_SAFE_EQUALS || op == Operator::IN))
        return evaluatePredicateBloomFilter(*bloom_filter, has_null);
    return result;
}


class ExpressionTree
{
public:
    enum class Operator : UInt8
    {
        OR,
        AND,
        NOT,
        LEAF,
        CONSTANT,
    };

    explicit ExpressionTree(Operator op_) : op(op_) {}
    explicit ExpressionTree(TruthValue constant_) : op(Operator::CONSTANT), constant(constant_) {}
    explicit ExpressionTree(size_t leaf_) : op(Operator::LEAF), leaf(leaf_) {}
    ExpressionTree(Operator op_, std::vector<ExpressionTreePtr> children_) : op(op_), children(std::move(children_)) {}

    TruthValue evaluate(const std::vector<TruthValue> & leaves) const
    {
        switch (op)
        {
            case Operator::OR:
            {
                TruthValue result = children.at(0)->evaluate(leaves);
                for (size_t i = 1; i < children.size() && !isNeeded(result); ++i)
                    result = children[i]->evaluate(leaves) || result;
                return result;
            }
            case Operator::AND:
            {
                TruthValue result = children.at(0)->evaluate(leaves);
                for (size_t i = 1; i < children.size() && isNeeded(result); ++i)
                    result = children[i]->evaluate(leaves) && result;
                return result;
            }
            case Operator::NOT:
                return !children.at(0)->evaluate(leaves);
            case Operator::LEAF:
                return leaves.at(leaf);
            case Operator::CONSTANT:
                return constant;
        }
        return TruthValue::YES_NO_NULL;
    }

    Operator op;
    TruthValue constant = TruthValue::YES_NO_NULL;
    size_t leaf = 0;
    std::vector<ExpressionTreePtr> children;
};


SearchArgument::SearchArgument(ExpressionTreePtr root_, std::vector<PredicateLeaf> leaves_)
    : root(std::move(root_)), leaves(std::move(leaves_))
{
}

TruthValue SearchArgument::evaluate(const std::vector<TruthValue> & leaf_values) const
{
    return root ? root->evaluate(leaf_values) : TruthValue::YES_NO_NULL;
}


namespace
{

using Op = ExpressionTree::Operator;

/// Pushes the negations down to the leaves and removes the double negations.
ExpressionTreePtr pushDownNot(ExpressionTreePtr root)
{
    if (root->op == Op::NOT)
    {
        ExpressionTreePtr child = root->children.at(0);
        switch (child->op)
        {
            case Op::NOT:
                return pushDownNot(child->children.at(0));
            case Op::CONSTANT:
                return std::make_shared<ExpressionTree>(!child->constant);
            case Op::AND:
            case Op::OR:
            {
                auto result = std::make_shared<ExpressionTree>(child->op == Op::AND ? Op::OR : Op::AND);
                for (const auto & kid : child->children)
                    result->children.push_back(pushDownNot(std::make_shared<ExpressionTree>(Op::NOT, std::vector<ExpressionTreePtr>{kid})));
                return result;
            }
            case Op::LEAF:
                break;
        }
    }
    else
    {
        for (auto & kid : root->children)
            kid = pushDownNot(kid);
    }
    return root;
}

/// Removes the MAYBE (YES_NO_NULL) constants: they are dropped from AND and they replace OR.
ExpressionTreePtr foldMaybe(ExpressionTreePtr expr)
{
    for (auto & kid : expr->children)
    {
        ExpressionTreePtr child = foldMaybe(kid);
        if (child->op == Op::CONSTANT && child->constant == TruthValue::YES_NO_NULL)
        {
            if (expr->op == Op::AND)
                kid = nullptr;
            else if (expr->op == Op::OR)
                return child;
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Got a maybe as a child of a NOT in ORC search argument");
        }
        else
            kid = child;
    }

    if (!expr->children.empty())
    {
        std::erase(expr->children, nullptr);
        if (expr->children.empty())
            return std::make_shared<ExpressionTree>(TruthValue::YES_NO_NULL);
    }
    return expr;
}

/// Converts multi-level ANDs and ORs into single level ones.
ExpressionTreePtr flatten(ExpressionTreePtr root)
{
    std::vector<ExpressionTreePtr> nodes;
    for (auto & kid : root->children)
    {
        ExpressionTreePtr child = flatten(kid);
        if (child->op == root->op && child->op != Op::NOT)
            nodes.insert(nodes.end(), child->children.begin(), child->children.end());
        else
            nodes.push_back(child);
    }
    root->children = std::move(nodes);
    if ((root->op == Op::OR || root->op == Op::AND) && root->children.size() == 1)
        return root->children[0];
    return root;
}

ExpressionTreePtr copyTree(const ExpressionTreePtr & node)
{
    auto result = std::make_shared<ExpressionTree>(*node);
    return result;
}

void generateAllCombinations(
    std::vector<ExpressionTreePtr> & result, const std::vector<ExpressionTreePtr> & and_list, size_t and_index, const std::vector<ExpressionTreePtr> & non_and_list)
{
    const auto & kids = and_list[and_index]->children;
    if (result.empty())
    {
        for (const auto & kid : kids)
        {
            auto node = std::make_shared<ExpressionTree>(Op::OR);
            for (const auto & non_and : non_and_list)
                node->children.push_back(copyTree(non_and));
            node->children.push_back(kid);
            result.push_back(node);
        }
    }
    else
    {
        std::vector<ExpressionTreePtr> work = std::move(result);
        result.clear();
        for (const auto & kid : kids)
        {
            for (const auto & node : work)
            {
                auto copy = copyTree(node);
                copy->children.push_back(kid);
                result.push_back(copy);
            }
        }
    }
    if (and_index + 1 < and_list.size())
        generateAllCombinations(result, and_list, and_index + 1, non_and_list);
}

constexpr size_t CNF_COMBINATIONS_THRESHOLD = 256;

/// Converts the expression into the conjunctive normal form, if it is not too large.
ExpressionTreePtr convertToCNF(ExpressionTreePtr root)
{
    for (auto & kid : root->children)
        kid = convertToCNF(kid);

    if (root->op != Op::OR)
        return root;

    std::vector<ExpressionTreePtr> non_and_list;
    std::vector<ExpressionTreePtr> and_list;
    for (const auto & child : root->children)
    {
        if (child->op == Op::AND)
            and_list.push_back(child);
        else if (child->op == Op::OR)
            non_and_list.insert(non_and_list.end(), child->children.begin(), child->children.end());
        else
            non_and_list.push_back(child);
    }

    if (and_list.empty())
        return root;

    size_t num_combinations = 1;
    for (const auto & node : and_list)
    {
        num_combinations *= node->children.size();
        if (num_combinations > CNF_COMBINATIONS_THRESHOLD)
            return std::make_shared<ExpressionTree>(TruthValue::YES_NO_NULL);
    }

    auto result = std::make_shared<ExpressionTree>(Op::AND);
    generateAllCombinations(result->children, and_list, 0, non_and_list);
    return result;
}

void collectUsedLeaves(const ExpressionTreePtr & node, std::vector<size_t> & reorder, size_t & next)
{
    if (node->op == Op::LEAF)
    {
        if (reorder[node->leaf] == std::numeric_limits<size_t>::max())
            reorder[node->leaf] = next++;
        return;
    }
    for (const auto & kid : node->children)
        collectUsedLeaves(kid, reorder, next);
}

/// The trees can share nodes after the CNF conversion, so rewrite every leaf node exactly once.
void rewriteLeaves(const ExpressionTreePtr & root, const std::vector<size_t> & reorder)
{
    std::unordered_set<ExpressionTree *> leaves;
    std::deque<ExpressionTree *> nodes{root.get()};
    while (!nodes.empty())
    {
        ExpressionTree * node = nodes.front();
        nodes.pop_front();
        if (node->op == Op::LEAF)
            leaves.insert(node);
        else
            for (const auto & kid : node->children)
                nodes.push_back(kid.get());
    }
    for (auto * leaf : leaves)
        leaf->leaf = reorder[leaf->leaf];
}

}


SearchArgumentBuilder::SearchArgumentBuilder()
{
    root = std::make_shared<ExpressionTree>(Op::AND);
    stack.push_back(root);
}

SearchArgumentBuilder & SearchArgumentBuilder::start(UInt8 op)
{
    auto node = std::make_shared<ExpressionTree>(static_cast<Op>(op));
    stack.back()->children.push_back(node);
    stack.push_back(node);
    return *this;
}

SearchArgumentBuilder & SearchArgumentBuilder::startOr() { return start(static_cast<UInt8>(Op::OR)); }
SearchArgumentBuilder & SearchArgumentBuilder::startAnd() { return start(static_cast<UInt8>(Op::AND)); }
SearchArgumentBuilder & SearchArgumentBuilder::startNot() { return start(static_cast<UInt8>(Op::NOT)); }

SearchArgumentBuilder & SearchArgumentBuilder::end()
{
    auto current = stack.back();
    stack.pop_back();
    if (stack.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unbalanced end() in ORC search argument builder");
    if (current->children.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot create an expression with no children in ORC search argument builder");
    if (current->op == Op::NOT && current->children.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A NOT expression must have exactly one child in ORC search argument builder");
    return *this;
}

SearchArgumentBuilder & SearchArgumentBuilder::literal(TruthValue value)
{
    stack.back()->children.push_back(std::make_shared<ExpressionTree>(value));
    return *this;
}

SearchArgumentBuilder & SearchArgumentBuilder::addLeaf(PredicateLeaf leaf)
{
    size_t index = leaves.size();
    leaves.push_back(std::move(leaf));
    stack.back()->children.push_back(std::make_shared<ExpressionTree>(index));
    return *this;
}

SearchArgumentBuilder & SearchArgumentBuilder::lessThan(UInt64 column_id, PredicateDataType type, Literal value)
{
    return addLeaf(PredicateLeaf(PredicateLeaf::Operator::LESS_THAN, type, column_id, {std::move(value)}));
}

SearchArgumentBuilder & SearchArgumentBuilder::lessThanEquals(UInt64 column_id, PredicateDataType type, Literal value)
{
    return addLeaf(PredicateLeaf(PredicateLeaf::Operator::LESS_THAN_EQUALS, type, column_id, {std::move(value)}));
}

SearchArgumentBuilder & SearchArgumentBuilder::equals(UInt64 column_id, PredicateDataType type, Literal value)
{
    return addLeaf(PredicateLeaf(PredicateLeaf::Operator::EQUALS, type, column_id, {std::move(value)}));
}

SearchArgumentBuilder & SearchArgumentBuilder::nullSafeEquals(UInt64 column_id, PredicateDataType type, Literal value)
{
    return addLeaf(PredicateLeaf(PredicateLeaf::Operator::NULL_SAFE_EQUALS, type, column_id, {std::move(value)}));
}

SearchArgumentBuilder & SearchArgumentBuilder::in(UInt64 column_id, PredicateDataType type, const std::vector<Literal> & values)
{
    if (values.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot create an IN expression with no arguments in ORC search argument builder");
    return addLeaf(PredicateLeaf(PredicateLeaf::Operator::IN, type, column_id, values));
}

SearchArgumentBuilder & SearchArgumentBuilder::isNull(UInt64 column_id, PredicateDataType type)
{
    return addLeaf(PredicateLeaf(PredicateLeaf::Operator::IS_NULL, type, column_id, {}));
}

SearchArgumentBuilder & SearchArgumentBuilder::between(UInt64 column_id, PredicateDataType type, Literal lower, Literal upper)
{
    return addLeaf(PredicateLeaf(PredicateLeaf::Operator::BETWEEN, type, column_id, {std::move(lower), std::move(upper)}));
}

std::unique_ptr<SearchArgument> SearchArgumentBuilder::build()
{
    if (stack.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to end {} operations in ORC search argument builder", stack.size() - 1);
    if (root->children.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty ORC search argument");

    root = pushDownNot(root);
    root = foldMaybe(root);
    root = flatten(root);
    root = convertToCNF(root);
    root = flatten(root);

    std::vector<size_t> reorder(leaves.size(), std::numeric_limits<size_t>::max());
    size_t num_used = 0;
    collectUsedLeaves(root, reorder, num_used);
    rewriteLeaves(root, reorder);

    std::vector<PredicateLeaf> used_leaves;
    used_leaves.reserve(num_used);
    std::vector<const PredicateLeaf *> by_new_index(num_used);
    for (size_t i = 0; i < leaves.size(); ++i)
        if (reorder[i] != std::numeric_limits<size_t>::max())
            by_new_index[reorder[i]] = &leaves[i];
    for (const auto * leaf : by_new_index)
        used_leaves.push_back(*leaf);

    return std::make_unique<SearchArgument>(root, std::move(used_leaves));
}


namespace
{

constexpr UInt32 MURMUR3_DEFAULT_SEED = 104729;

inline UInt64 rotl64(UInt64 x, int r)
{
    return (x << r) | (x >> (64 - r));
}

inline UInt64 fmix64(UInt64 value)
{
    value ^= (value >> 33);
    value *= 0xff51afd7ed558ccdULL;
    value ^= (value >> 33);
    value *= 0xc4ceb9fe1a85ec53ULL;
    value ^= (value >> 33);
    return value;
}

/// The 64-bit variant of Murmur3 used by ORC (the lower half of the 128-bit hash with one state).
UInt64 murmur3Hash64(const UInt8 * data, size_t size)
{
    UInt64 h = MURMUR3_DEFAULT_SEED;
    const UInt64 c1 = 0x87c37b91114253d5ULL;
    const UInt64 c2 = 0x4cf5ad432745937fULL;
    size_t blocks = size >> 3;
    for (size_t i = 0; i < blocks; ++i)
    {
        UInt64 k = 0;
        memcpy(&k, data + i * 8, 8);
        if constexpr (std::endian::native == std::endian::big)
            k = std::byteswap(k);
        k *= c1;
        k = rotl64(k, 31);
        k *= c2;
        h ^= k;
        h = rotl64(h, 27);
        h = h * 5 + 0x52dce729;
    }

    UInt64 k = 0;
    size_t idx = blocks << 3;
    size_t tail = size - idx;
    for (size_t i = tail; i > 0; --i)
        k ^= static_cast<UInt64>(data[idx + i - 1]) << ((i - 1) * 8);
    if (tail)
    {
        k *= c1;
        k = rotl64(k, 31);
        k *= c2;
        h ^= k;
    }

    h ^= static_cast<UInt32>(size);
    return fmix64(h);
}

/// Thomas Wang's 64-bit integer hash, as in the reference implementation.
Int64 getLongHash(Int64 key)
{
    UInt64 k = static_cast<UInt64>(key);
    k = (~k) + (k << 21);
    k = k ^ static_cast<UInt64>(static_cast<Int64>(k) >> 24);
    k = (k + (k << 3)) + (k << 8);
    k = k ^ static_cast<UInt64>(static_cast<Int64>(k) >> 14);
    k = (k + (k << 2)) + (k << 4);
    k = k ^ static_cast<UInt64>(static_cast<Int64>(k) >> 28);
    k = k + (k << 31);
    return static_cast<Int64>(k);
}

}

BloomFilter::BloomFilter(const Proto::BloomFilter & proto) : num_hash_functions(proto.num_hash_functions)
{
    /// The writers use a few hash functions (the optimal number for the false positive probability),
    /// a huge number from a corrupt file would make every probe extremely slow.
    static constexpr UInt32 MAX_HASH_FUNCTIONS = 256;
    if (num_hash_functions == 0 || num_hash_functions > MAX_HASH_FUNCTIONS)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid number of hash functions {} of a bloom filter in ORC file, must be from 1 to {}",
            num_hash_functions, MAX_HASH_FUNCTIONS);
    const std::string & bitset = proto.utf8bitset.value();
    if (bitset.empty() || bitset.size() % 8 != 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "The size of a bloom filter in ORC file must be a positive multiple of 8 bytes");
    bits.resize(bitset.size() / 8);
    memcpy(bits.data(), bitset.data(), bitset.size());
    if constexpr (std::endian::native == std::endian::big)
        for (auto & word : bits)
            word = std::byteswap(word);
}

bool BloomFilter::testHash(Int64 hash) const
{
    Int32 hash1 = static_cast<Int32>(hash & 0xffffffff);
    Int32 hash2 = static_cast<Int32>(static_cast<UInt64>(hash) >> 32);
    UInt64 num_bits = bits.size() * 64;
    for (UInt32 i = 1; i <= num_hash_functions; ++i)
    {
        Int32 combined = static_cast<Int32>(static_cast<UInt32>(hash1) + i * static_cast<UInt32>(hash2));
        if (combined < 0)
            combined = ~combined;
        UInt64 pos = static_cast<UInt64>(combined) % num_bits;
        if (!(bits[pos >> 6] & (1ULL << (pos % 64))))
            return false;
    }
    return true;
}

bool BloomFilter::testLong(Int64 value) const
{
    return testHash(getLongHash(value));
}

bool BloomFilter::testDouble(double value) const
{
    return testLong(std::bit_cast<Int64>(value));
}

bool BloomFilter::testBytes(const char * data, size_t size) const
{
    return testHash(static_cast<Int64>(murmur3Hash64(reinterpret_cast<const UInt8 *>(data), size)));
}

}
