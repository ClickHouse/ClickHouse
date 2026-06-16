#include <Analyzer/Passes/LikePerfectAffixRewritePass.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/IdentifierNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Core/Settings.h>
#include <Common/StringUtils.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>

#include <boost/algorithm/string.hpp>

namespace DB
{
namespace Setting
{
    extern const SettingsBool optimize_rewrite_like_perfect_affix;
}

namespace
{

/** Rewrite LIKE expressions with perfect affix (prefix or suffix) to startsWith or endsWith functions.
  * The scope of the rewrite:
  *     - `col LIKE 'Prefix%'`: as `startsWith(col, 'Prefix')`
  *     - `col LIKE '%Suffix'`: as `endsWith(col, 'Suffix')`
  *     - `NOT LIKE `: negating above functions with `NOT`
  * Out of scope:
  *     - `ILIKE` is not rewritten until starts/endsWithCaseInsensitive is implemented, because lower() or left() based rewrites are suboptimal
  * Type requirement on the left operand, `col`:
  *     - `startsWith`: Only resolved column of type `String`, `FixedString`, or nested types of these:
  *         - `LowCardinality(S)`, `Nullable(S)`, `LowCardinality(Nullable(S))` where `S` is either `String` or `FixedString`.
  *     - `endsWith`: same as `startsWith` except `FixedString` (nested or not), because `c LIKE '%suffix\0'` is not equal to `endsWith(c, 'suffix\0')`.
  */
class LikePerfectAffixRewriteVisitor : public InDepthQueryTreeVisitorWithContext<LikePerfectAffixRewriteVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<LikePerfectAffixRewriteVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_rewrite_like_perfect_affix])
            return;

        auto * function_node = node->as<FunctionNode>();
        if (!function_node)
            return;

        const String & function_name = function_node->getFunctionName();
        const bool is_like = (function_name == "like");
        const bool is_not_like = (function_name == "notLike");
        if (!(is_like || is_not_like))
            return;

        auto & args = function_node->getArguments().getNodes();
        if (args.size() != 2)
            return;

        /// Do not rewrite for non-column left hand side
        if (args[0]->getNodeType() != QueryTreeNodeType::COLUMN)
            return;

        /// Extract affix (prefix or suffix) and check if suitable for rewrite
        auto * pattern_constant = args[1]->as<ConstantNode>();
        if (!pattern_constant || !isString(pattern_constant->getResultType()))
            return;

        auto pattern = pattern_constant->getValue().safeGet<String>();
        const bool is_suffix = pattern.starts_with("%");

        /// Do not rewrite for
        /// (1) non-string related type (String, FixedString, or nested within LowCardinality or Nullable or both)
        /// (2) Suffix matching on FixedString, which requires trimming trailing null chars
        if (!isResultTypeSupported(args[0]->getResultType(), is_suffix))
            return;

        /// Only rewrite for perfect prefix or suffix
        /// Suffix is prefix in reverse
        if (is_suffix)
            std::reverse(pattern.begin(), pattern.end());

        auto [affix, is_perfect] = extractFixedPrefixFromLikePattern(pattern, true);
        if (!is_perfect || affix.empty())
            return;

        if (is_suffix)
            std::reverse(affix.begin(), affix.end());

        auto affix_constant = std::make_shared<ConstantNode>(std::move(affix));

        /// Create startsWith/endsWith function
        FunctionNodePtr new_node = operation(is_suffix ? "endsWith" : "startsWith", args[0], affix_constant);
        if (is_not_like)
            new_node = operation("not", new_node);

        /// Replpace the original LIKE node
        chassert(new_node != nullptr, "should have been created");
        chassert(new_node->getResultType()->getTypeId() == node->getResultType()->getTypeId(), "Rewrite should preserve type");
        node = std::move(new_node);
    }
private:
    template<typename... Args>
    requires (std::is_convertible_v<Args, QueryTreeNodePtr> && ...) && (sizeof...(Args) >= 1)
    FunctionNodePtr operation(const String & op, Args&&... operands)
    {
        auto op_function = std::make_shared<FunctionNode>(op);
        auto op_resolver = FunctionFactory::instance().get(op, getContext());
        op_function->getArguments().getNodes() = {std::forward<Args>(operands)...};
        op_function->resolveAsFunction(op_resolver);
        return op_function;
    }

    bool isStr(const DataTypePtr & type, const bool for_suffix)
    {
        /// Do not rewrite for fixedstring with suffix, because `col LIKE '%suffix\0'` is not `endsWith(col, 'suffix\0')`
        if (for_suffix)
            return isString(type);
        else
            return isStringOrFixedString(type);
    }

    bool isResultTypeSupported(const DataTypePtr & type, const bool for_suffix)
    {
        return isStr(type, for_suffix) || isNullableStr(type, for_suffix) || isLowCardinalityMaybeNullableStr(type, for_suffix);
    }

    bool isLowCardinalityMaybeNullableStr(const DataTypePtr & type, const bool for_suffix)
    {
        if (const DataTypeLowCardinality * type_low_cardinality = typeid_cast<const DataTypeLowCardinality *>(type.get()))
        {
            DataTypePtr type_dictionary = type_low_cardinality->getDictionaryType();
            return isStr(type_dictionary, for_suffix) || isNullableStr(type_dictionary, for_suffix);
        }
        return false;
    }

    bool isNullableStr(const DataTypePtr & type, const bool for_suffix)
    {
        if (const DataTypeNullable * type_nullable = typeid_cast<const DataTypeNullable *>(type.get()))
        {
            DataTypePtr type_nested = type_nullable->getNestedType();
            return isStr(type_nested, for_suffix);
        }
        return false;
    }
};

}

void LikePerfectAffixRewritePass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    LikePerfectAffixRewriteVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}

