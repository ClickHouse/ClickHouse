#include <Interpreters/getCustomKeyFilterForParallelReplicas.h>

#include <Core/Settings.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSampleRatio.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>

#include <Interpreters/Context.h>


#include <boost/rational.hpp>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_query_size;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
    extern const int INVALID_SETTING_VALUE;
}

ASTPtr getCustomKeyFilterForParallelReplica(
    size_t replicas_count,
    size_t replica_num,
    ASTPtr custom_key_ast,
    ParallelReplicasCustomKeyFilter filter,
    const ColumnsDescription & columns,
    const ContextPtr & context)
{
    chassert(replicas_count > 1);
    if (filter.filter_type == ParallelReplicasCustomKeyFilterType::DEFAULT)
    {
        // first we do modulo with replica count
        auto modulo_function = makeASTFunction("positiveModulo", custom_key_ast, std::make_shared<ASTLiteral>(replicas_count));

        /// then we compare result to the current replica number (offset)
        auto equals_function = makeASTFunction("equals", std::move(modulo_function), std::make_shared<ASTLiteral>(replica_num));

        return equals_function;
    }

    chassert(filter.filter_type == ParallelReplicasCustomKeyFilterType::RANGE);

    KeyDescription custom_key_description
        = KeyDescription::getKeyFromAST(custom_key_ast, columns, context);

    using RelativeSize = boost::rational<ASTSampleRatio::BigNum>;

    RelativeSize range_upper = RelativeSize(0);
    RelativeSize range_lower = RelativeSize(filter.range_lower);
    DataTypePtr custom_key_column_type = custom_key_description.data_types[0];

    if (custom_key_description.data_types.size() == 1)
    {
        if (typeid_cast<const DataTypeUInt64 *>(custom_key_column_type.get()))
        {
            range_upper = filter.range_upper > 0 ? RelativeSize(filter.range_upper) + RelativeSize(1)
                                                 : RelativeSize(std::numeric_limits<UInt64>::max()) + RelativeSize(1);
            if (range_upper > RelativeSize(std::numeric_limits<UInt64>::max()) + RelativeSize(1))
                throw Exception(
                    ErrorCodes::INVALID_SETTING_VALUE,
                    "Invalid custom key range upper bound: {}. Value must be smaller than custom key column type (UInt64) max value",
                    range_upper);
        }
        else if (typeid_cast<const DataTypeUInt32 *>(custom_key_column_type.get()))
        {
            range_upper = filter.range_upper > 0 ? RelativeSize(filter.range_upper) + RelativeSize(1)
                                                 : RelativeSize(std::numeric_limits<UInt32>::max()) + RelativeSize(1);
            if (range_upper > RelativeSize(std::numeric_limits<UInt32>::max()) + RelativeSize(1))
                throw Exception(
                    ErrorCodes::INVALID_SETTING_VALUE,
                    "Invalid custom key range upper bound: {}. Value must be smaller than custom key column type (UInt32) max value",
                    range_upper);
        }
        else if (typeid_cast<const DataTypeUInt16 *>(custom_key_column_type.get()))
        {
            range_upper = filter.range_upper > 0 ? RelativeSize(filter.range_upper) + RelativeSize(1)
                                                 : RelativeSize(std::numeric_limits<UInt16>::max()) + RelativeSize(1);
            if (range_upper > RelativeSize(std::numeric_limits<UInt16>::max()) + RelativeSize(1))
                throw Exception(
                    ErrorCodes::INVALID_SETTING_VALUE,
                    "Invalid custom key range upper bound: {}. Value must be smaller than custom key column type (UInt16) max value",
                    range_upper);
        }
        else if (typeid_cast<const DataTypeUInt8 *>(custom_key_column_type.get()))
        {
            range_upper = filter.range_upper > 0 ? RelativeSize(filter.range_upper) + RelativeSize(1)
                                                 : RelativeSize(std::numeric_limits<UInt8>::max()) + RelativeSize(1);
            if (range_upper > RelativeSize(std::numeric_limits<UInt8>::max()) + RelativeSize(1))
                throw Exception(
                    ErrorCodes::INVALID_SETTING_VALUE,
                    "Invalid custom key range upper bound: {}. Value must be smaller than custom key column type (UInt8) max value",
                    range_upper);
        }
    }

    if (range_upper == RelativeSize(0))
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
            "Invalid custom key column type: {}. Must be one unsigned integer type",
            custom_key_column_type->getName());

    if (range_lower >= range_upper)
        throw Exception(
            ErrorCodes::INVALID_SETTING_VALUE,
            "Invalid custom key filter range: Range upper bound {} must be larger than range lower bound {}",
            range_lower,
            range_upper);

    RelativeSize size_of_universum = range_upper - range_lower;

    if (size_of_universum <= RelativeSize(replicas_count))
        throw Exception(
            ErrorCodes::INVALID_SETTING_VALUE, "Invalid custom key filter range: Range must be larger than than the number of replicas");

    RelativeSize relative_range_size = RelativeSize(1) / replicas_count;
    RelativeSize relative_range_offset = relative_range_size * RelativeSize(replica_num);

    /// Calculate the half-interval of `[lower, upper)` column values.
    bool has_lower_limit = false;
    bool has_upper_limit = false;

    RelativeSize lower_limit_rational = range_lower + relative_range_offset * size_of_universum;
    RelativeSize upper_limit_rational = range_lower + (relative_range_offset + relative_range_size) * size_of_universum;

    UInt64 lower = boost::rational_cast<ASTSampleRatio::BigNum>(lower_limit_rational);
    UInt64 upper = boost::rational_cast<ASTSampleRatio::BigNum>(upper_limit_rational);

    if (lower_limit_rational > range_lower)
        has_lower_limit = true;

    if (upper_limit_rational < range_upper)
        has_upper_limit = true;

    chassert(has_lower_limit || has_upper_limit);

    /// Let's add the conditions to cut off something else when the index is scanned again and when the request is processed.
    std::shared_ptr<ASTFunction> lower_function;
    std::shared_ptr<ASTFunction> upper_function;

    if (has_lower_limit)
    {
        lower_function = makeASTFunction("greaterOrEquals", custom_key_ast, std::make_shared<ASTLiteral>(lower));

        if (!has_upper_limit)
            return lower_function;
    }

    if (has_upper_limit)
    {
        upper_function = makeASTFunction("less", custom_key_ast, std::make_shared<ASTLiteral>(upper));

        if (!has_lower_limit)
            return upper_function;
    }

    chassert(upper_function && lower_function);

    return makeASTFunction("and", std::move(lower_function), std::move(upper_function));
}

ASTPtr parseCustomKeyForTable(const String & custom_key, const Context & context)
{
    /// Try to parse expression
    ParserExpression parser;
    const auto & settings = context.getSettingsRef();
    return parseQuery(
        parser,
        custom_key.data(),
        custom_key.data() + custom_key.size(),
        "parallel replicas custom key",
        settings[Setting::max_query_size],
        settings[Setting::max_parser_depth],
        settings[Setting::max_parser_backtracks]);
}

}
