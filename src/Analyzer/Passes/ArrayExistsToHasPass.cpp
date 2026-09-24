#include <Analyzer/Passes/ArrayExistsToHasPass.h>

#include <Functions/array/has.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/LambdaNode.h>
#include <Analyzer/Utils.h>

#include <Common/likePatternToRegexp.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/getLeastSupertype.h>
#include <Interpreters/ITokenizer.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool optimize_rewrite_array_exists_to_has;
}

namespace
{

/// `equals` compares the string family zero-padded, so a needle whose extra bytes are all NUL
/// still matches a narrower element. `has` compares either the String supertype, which strips
/// trailing NULs, or raw Fields, which are not padded. The two therefore agree only when the
/// element and needle types are identical, and a container agrees only when its members do.
bool stringFamilyPairIsNotEqualityEquivalent(const DataTypePtr & element_type, const DataTypePtr & needle_type)
{
    auto element = removeNullable(removeLowCardinality(element_type));
    auto needle = removeNullable(removeLowCardinality(needle_type));

    const auto * element_tuple = typeid_cast<const DataTypeTuple *>(element.get());
    const auto * needle_tuple = typeid_cast<const DataTypeTuple *>(needle.get());
    if (element_tuple && needle_tuple)
    {
        const auto & element_elements = element_tuple->getElements();
        const auto & needle_elements = needle_tuple->getElements();
        if (element_elements.size() != needle_elements.size())
            return false;

        for (size_t i = 0; i < element_elements.size(); ++i)
            if (stringFamilyPairIsNotEqualityEquivalent(element_elements[i], needle_elements[i]))
                return true;

        return false;
    }

    const auto * element_array = typeid_cast<const DataTypeArray *>(element.get());
    const auto * needle_array = typeid_cast<const DataTypeArray *>(needle.get());
    if (element_array && needle_array)
        return stringFamilyPairIsNotEqualityEquivalent(element_array->getNestedType(), needle_array->getNestedType());

    const auto * element_map = typeid_cast<const DataTypeMap *>(element.get());
    const auto * needle_map = typeid_cast<const DataTypeMap *>(needle.get());
    if (element_map && needle_map)
        return stringFamilyPairIsNotEqualityEquivalent(element_map->getKeyType(), needle_map->getKeyType())
            || stringFamilyPairIsNotEqualityEquivalent(element_map->getValueType(), needle_map->getValueType());

    return isStringOrFixedString(element) && isStringOrFixedString(needle) && !element->equals(*needle);
}

class RewriteArrayExistsToHasVisitor : public InDepthQueryTreeVisitorWithContext<RewriteArrayExistsToHasVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<RewriteArrayExistsToHasVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_rewrite_array_exists_to_has])
            return;

        auto * array_exists_function_node = node->as<FunctionNode>();
        if (!array_exists_function_node || array_exists_function_node->getFunctionName() != "arrayExists")
            return;

        auto & array_exists_function_arguments_nodes = array_exists_function_node->getArguments().getNodes();
        if (array_exists_function_arguments_nodes.size() != 2)
            return;

        /// lambda function must be like: x -> x = elem
        auto * lambda_node = array_exists_function_arguments_nodes[0]->as<LambdaNode>();
        if (!lambda_node)
            return;

        const auto & lambda_argument_names = lambda_node->getArguments().getNames();
        if (lambda_argument_names.size() != 1)
            return;

        const auto & lambda_argument_name = lambda_argument_names[0];
        auto lambda_arguments_node = lambda_node->getArgumentsTyped();

        /// The lambda parameter is referenced in the body as a column sourced from the lambda arguments node.
        auto is_lambda_argument = [&](const QueryTreeNodePtr & argument_node)
        {
            const auto * column_node = argument_node->as<ColumnNode>();
            return column_node && column_node->getColumnName() == lambda_argument_name
                && column_node->getColumnSourceOrNull() == lambda_arguments_node;
        };

        if (tryRewriteToHasTokenFunction(*array_exists_function_node, lambda_node->getExpression(), is_lambda_argument))
            return;

        auto * filter_node = lambda_node->getExpression()->as<FunctionNode>();
        if (!filter_node || filter_node->getFunctionName() != "equals")
            return;

        const auto & filter_arguments_nodes = filter_node->getArguments().getNodes();
        if (filter_arguments_nodes.size() != 2)
            return;

        const auto & filter_lhs_argument_node = filter_arguments_nodes[0];
        auto filter_lhs_argument_node_type = filter_lhs_argument_node->getNodeType();

        const auto & filter_rhs_argument_node = filter_arguments_nodes[1];
        auto filter_rhs_argument_node_type = filter_rhs_argument_node->getNodeType();

        QueryTreeNodePtr has_constant_element_argument;

        if (filter_lhs_argument_node_type == QueryTreeNodeType::COLUMN &&
            filter_rhs_argument_node_type == QueryTreeNodeType::CONSTANT &&
            is_lambda_argument(filter_lhs_argument_node))
        {
            /// Rewrite arrayExists(x -> x = elem, arr) -> has(arr, elem)
            has_constant_element_argument = filter_rhs_argument_node;
        }
        else if (filter_lhs_argument_node_type == QueryTreeNodeType::CONSTANT &&
            filter_rhs_argument_node_type == QueryTreeNodeType::COLUMN &&
            is_lambda_argument(filter_rhs_argument_node))
        {
            /// Rewrite arrayExists(x -> elem = x, arr) -> has(arr, elem)
            has_constant_element_argument = filter_lhs_argument_node;
        }
        else
        {
            return;
        }

        /// Check that the types are compatible for the `has` function.
        /// The `has` function requires that the array element type and the search element type
        /// have a common supertype. The `equals` function in the lambda is more permissive
        /// (e.g. it can compare Date with String via implicit conversions), so we must verify
        /// compatibility before rewriting.
        const auto * array_type = typeid_cast<const DataTypeArray *>(array_exists_function_arguments_nodes[1]->getResultType().get());
        if (!array_type)
            return;

        auto nested_type = removeNullable(removeLowCardinality(array_type->getNestedType()));
        auto constant_type = removeNullable(removeLowCardinality(has_constant_element_argument->getResultType()));

        /// Skip rewrite when the constant is NULL (either untyped or typed).
        /// arrayExists(x -> x = NULL, [NULL]) returns 0 because equals(NULL, NULL) is NULL,
        /// and arrayExists treats non-true values as false.
        /// But has([NULL], NULL) returns 1, so the rewrite would change semantics.
        /// This also applies to typed NULLs like CAST(NULL AS Nullable(Int8)).
        if (isNothing(constant_type))
            return;

        const auto * constant_node = has_constant_element_argument->as<ConstantNode>();
        if (constant_node && constant_node->getValue().isNull())
            return;

        /// Such a pair has a supertype, so the check below admits it, but the two spellings
        /// still disagree.
        if (stringFamilyPairIsNotEqualityEquivalent(nested_type, constant_type))
            return;

        bool types_compatible = (isNativeNumber(nested_type) || isEnum(nested_type)) && isNativeNumber(constant_type);
        if (!types_compatible)
            types_compatible = tryGetLeastSupertype(DataTypes{nested_type, constant_type}) != nullptr;

        if (!types_compatible)
            return;

        auto has_function = createInternalFunctionHasOverloadResolver();

        array_exists_function_arguments_nodes[0] = std::move(array_exists_function_arguments_nodes[1]);
        array_exists_function_arguments_nodes[1] = std::move(has_constant_element_argument);
        array_exists_function_node->resolveAsFunction(has_function->build(array_exists_function_node->getArgumentColumns()));
    }

private:
    /// Rewrite arrayExists(x -> f(x, c), tokens(input[, tokenizer])) to hasTokenLike/hasTokenMatch(input, pattern, tokenizer),
    /// which a text index on `input` can answer. Unlike hasTokenPrefix, they never apply the preprocessor of that index.
    bool tryRewriteToHasTokenFunction(FunctionNode & array_exists_function_node, const QueryTreeNodePtr & lambda_expression, const auto & is_lambda_argument)
    {
        auto & arguments = array_exists_function_node.getArguments().getNodes();
        const auto * tokens_function_node = arguments[1]->as<FunctionNode>();
        if (!tokens_function_node || tokens_function_node->getFunctionName() != "tokens")
            return false;

        /// With Nullable or LowCardinality input the rewritten function would not return UInt8.
        /// Tokenizer parameters in separate arguments, as in tokens(s, 'ngrams', 3), are not rewritten.
        const auto & tokens_arguments = tokens_function_node->getArguments().getNodes();
        if (tokens_arguments.empty() || tokens_arguments.size() > 2 || !isStringOrFixedString(tokens_arguments[0]->getResultType()))
            return false;

        /// The tokenizer is always passed, otherwise the function would take it from a text index on `input`.
        QueryTreeNodePtr tokenizer;
        if (tokens_arguments.size() == 2)
        {
            const auto * tokenizer_constant = tokens_arguments[1]->as<ConstantNode>();
            if (!tokenizer_constant || !isString(tokenizer_constant->getResultType()))
                return false;
            tokenizer = tokens_arguments[1];
        }
        else
        {
            tokenizer = std::make_shared<ConstantNode>(String(SplitByNonAlphaTokenizer::getExternalName()));
        }

        const auto * filter_node = lambda_expression->as<FunctionNode>();
        if (!filter_node)
            return false;

        bool is_position = false;
        if (filter_node->getFunctionName() == "greater")
        {
            const auto & greater_arguments = filter_node->getArguments().getNodes();
            const auto * zero_constant = greater_arguments.size() == 2 ? greater_arguments[1]->as<ConstantNode>() : nullptr;
            if (!zero_constant || zero_constant->getValue() != Field(UInt64(0)))
                return false;

            filter_node = greater_arguments[0]->as<FunctionNode>();
            if (!filter_node || filter_node->getFunctionName() != "position")
                return false;
            is_position = true;
        }

        const auto & filter_arguments = filter_node->getArguments().getNodes();
        if (filter_arguments.size() != 2 || !is_lambda_argument(filter_arguments[0]))
            return false;

        const auto * needle_constant = filter_arguments[1]->as<ConstantNode>();
        if (!needle_constant || !isString(needle_constant->getResultType()))
            return false;

        const auto & needle = needle_constant->getValue().safeGet<String>();
        const auto & filter_function_name = filter_node->getFunctionName();
        String function_name = "hasTokenLike";
        QueryTreeNodePtr pattern;

        if (is_position)
            pattern = std::make_shared<ConstantNode>("%" + escapeForLikePattern(needle) + "%");
        else if (filter_function_name == "startsWith")
            pattern = std::make_shared<ConstantNode>(escapeForLikePattern(needle) + "%");
        else if (filter_function_name == "endsWith")
            pattern = std::make_shared<ConstantNode>("%" + escapeForLikePattern(needle));
        else if (filter_function_name == "like")
            pattern = filter_arguments[1];
        else if (filter_function_name == "match")
        {
            function_name = "hasTokenMatch";
            pattern = filter_arguments[1];
        }
        else
            return false;

        arguments = {tokens_arguments[0], std::move(pattern), std::move(tokenizer)};
        resolveOrdinaryFunctionNodeByName(array_exists_function_node, function_name, getContext());
        return true;
    }
};

}

void RewriteArrayExistsToHasPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    RewriteArrayExistsToHasVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}
