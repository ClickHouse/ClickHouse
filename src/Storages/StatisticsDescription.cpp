#include <Storages/StatisticsDescription.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Interpreters/IdentifierSemantic.h>

#include <Poco/String.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int ILLEGAL_STATISTICS;
    extern const int LOGICAL_ERROR;
};

SingleStatisticsDescription & SingleStatisticsDescription::operator=(const SingleStatisticsDescription & other)
{
    if (this == &other)
        return *this;

    type = other.type;
    ast = other.ast ? other.ast->clone() : nullptr;

    return *this;
}

SingleStatisticsDescription & SingleStatisticsDescription::operator=(SingleStatisticsDescription && other) noexcept
{
    if (this == &other)
        return *this;

    type = std::exchange(other.type, StatisticsType{});
    ast = other.ast ? other.ast->clone() : nullptr;
    other.ast.reset();

    return *this;
}

StatisticsType stringToStatisticsType(String type)
{
    type = Poco::toLower(type);

    if (type == "tdigest")
        return StatisticsType::TDigest;
    if (type == "uniq")
        return StatisticsType::Uniq;
    if (type == "countmin")
        return StatisticsType::CountMinSketch;
    if (type == "minmax")
        return StatisticsType::MinMax;
    if (type == "basic")
        return StatisticsType::Basic;
    if (type == "uniq_v2")
        return StatisticsType::UniqV2;

    throw Exception(ErrorCodes::INCORRECT_QUERY, "Unknown statistics type: {}. Supported statistics types are 'basic', 'countmin', 'minmax', 'tdigest', 'uniq' and 'uniq_v2'", type);
}

String statisticsTypeToString(StatisticsType type)
{
    switch (type)
    {
        case StatisticsType::TDigest:
            return "TDigest";
        case StatisticsType::Uniq:
            return "Uniq";
        case StatisticsType::CountMinSketch:
            return "countmin";
        case StatisticsType::MinMax:
            return "minmax";
        case StatisticsType::Basic:
            return "basic";
        case StatisticsType::UniqV2:
            return "uniq_v2";
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown statistics type: {}. Supported statistics types are 'basic', 'countmin', 'minmax', 'tdigest', 'uniq' and 'uniq_v2'", type);
    }
}

String SingleStatisticsDescription::getTypeName() const
{
    return statisticsTypeToString(type);
}

SingleStatisticsDescription::SingleStatisticsDescription(StatisticsType type_, ASTPtr ast_)
    : type(type_), ast(ast_)
{}

namespace
{

size_t stringAllocatedBytes(const String & str)
{
    return str.capacity();
}

/// The exact size of an AST node is not known through `IAST`, so it is bounded from above.
/// The bound covers the node, the semantic object of an identifier, and the control blocks of the pointers.
constexpr size_t AST_NODE_SIZE_BOUND = 512;
static_assert(sizeof(ASTIdentifier) + sizeof(IdentifierSemanticImpl) + 2 * 4 * sizeof(void *) <= AST_NODE_SIZE_BOUND);
static_assert(sizeof(ASTFunction) <= AST_NODE_SIZE_BOUND);
static_assert(sizeof(ASTExpressionList) <= AST_NODE_SIZE_BOUND);
static_assert(sizeof(ASTLiteral) <= AST_NODE_SIZE_BOUND);

size_t astAllocatedBytes(const IAST & ast)
{
    size_t res = AST_NODE_SIZE_BOUND + ast.children.capacity() * sizeof(ASTPtr);
    if (const auto * identifier = ast.as<ASTIdentifier>())
    {
        res += stringAllocatedBytes(identifier->full_name);
        res += identifier->name_parts.capacity() * sizeof(String);
        for (const auto & part : identifier->name_parts)
            res += stringAllocatedBytes(part);
    }
    else if (const auto * literal = ast.as<ASTLiteral>())
    {
        if (literal->value.getType() == Field::Types::String)
            res += stringAllocatedBytes(literal->value.safeGet<String>());
    }

    for (const auto & child : ast.children)
        res += astAllocatedBytes(*child);
    return res;
}

}

size_t SingleStatisticsDescription::allocatedBytes() const
{
    return ast ? astAllocatedBytes(*ast) : 0;
}

bool SingleStatisticsDescription::operator==(const SingleStatisticsDescription & other) const
{
    return type == other.type;
}

bool ColumnStatisticsDescription::operator==(const ColumnStatisticsDescription & other) const
{
    if (!data_type)
        return !other.data_type;

    if (!other.data_type)
        return false;

    return types_to_desc == other.types_to_desc && data_type->equals(*other.data_type);
}

bool ColumnStatisticsDescription::empty() const
{
    return types_to_desc.empty();
}

bool ColumnStatisticsDescription::contains(const String & stat_type) const
{
    return types_to_desc.contains(stringToStatisticsType(stat_type));
}

void ColumnStatisticsDescription::merge(const ColumnStatisticsDescription & other, const String & merging_column_name, DataTypePtr merging_column_type, bool if_not_exists)
{
    chassert(merging_column_type);

    data_type = merging_column_type;

    for (const auto & [stats_type, stats_desc]: other.types_to_desc)
    {
        if (!if_not_exists && types_to_desc.contains(stats_type))
        {
            throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Statistics type name {} has existed in column {}", stats_type, merging_column_name);
        }
        if (!types_to_desc.contains(stats_type))
            types_to_desc.emplace(stats_type, stats_desc);
    }
}

void ColumnStatisticsDescription::clear()
{
    types_to_desc.clear();
}

String ColumnStatisticsDescription::getNameForLogs() const
{
    String ret;
    for (const auto & [tp, desc] : types_to_desc)
    {
        ret += desc.getTypeName();
        ret += ",";
    }
    if (!ret.empty())
        ret.pop_back();
    return ret;
}


}
