#include <Analyzer/Resolve/EinsumLambdaRewrite.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/IdentifierNode.h>
#include <Analyzer/LambdaNode.h>
#include <Analyzer/ListNode.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>

#include <span>
#include <unordered_set>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// An element of a tensor in the expression: a chain of subscripts where some subscripts are indices.
struct TensorElement
{
    /// The array with the subscripts that are not indices applied to it.
    QueryTreeNodePtr tensor;
    /// The indices in the remaining subscripts, in their order.
    std::vector<size_t> indices;
};

void collectIdentifierNames(const IQueryTreeNode & node, std::unordered_set<String> & names)
{
    if (const auto * identifier = node.as<IdentifierNode>())
    {
        names.insert(identifier->getIdentifier().getFullName());
        for (const auto & part : identifier->getIdentifier().getParts())
            names.insert(part);
    }
    if (const auto * lambda = node.as<LambdaNode>())
        for (const auto & name : lambda->getArguments().getNames())
            names.insert(name);

    for (const auto & child : node.getChildren())
        if (child)
            collectIdentifierNames(*child, names);
}

/// A name that is not in `used_names`, for a parameter of a lambda function that must not shadow anything.
String getUnusedName(const String & prefix, const std::unordered_set<String> & used_names)
{
    String name = prefix;
    for (size_t suffix = 1; used_names.contains(name); ++suffix)
        name = prefix + std::to_string(suffix);
    return name;
}

const FunctionNode * asArrayElement(const IQueryTreeNode & node)
{
    const auto * function = node.as<FunctionNode>();
    if (function && function->getFunctionName() == "arrayElement" && function->getArguments().getNodes().size() == 2)
        return function;
    return nullptr;
}

QueryTreeNodePtr makeFunction(const String & name, QueryTreeNodes arguments)
{
    auto function = std::make_shared<FunctionNode>(name);
    function->getArguments().getNodes() = std::move(arguments);
    return function;
}

/** Applies the subscripts to the array, where `nullptr` means all elements of a dimension, like `arr[][1]` does.
  * The trailing `nullptr` subscripts are omitted: arr[1][][] is arr[1].
  */
QueryTreeNodePtr applySubscripts(QueryTreeNodePtr array, std::span<const QueryTreeNodePtr> subscripts, size_t depth = 0)
{
    for (size_t i = 0; i < subscripts.size(); ++i)
    {
        if (subscripts[i])
        {
            array = makeFunction("arrayElement", {std::move(array), subscripts[i]->clone()});
            continue;
        }

        auto rest = subscripts.subspan(i + 1);
        if (std::all_of(rest.begin(), rest.end(), [](const QueryTreeNodePtr & subscript) { return !subscript; }))
            return array;

        std::unordered_set<String> used_names;
        for (const auto & subscript : rest)
            if (subscript)
                collectIdentifierNames(*subscript, used_names);
        /// Different names for the nested lambdas, so that they are readable.
        static constexpr std::array<std::string_view, 4> names{"x", "y", "z", "w"};
        String name = getUnusedName(String(names[depth % names.size()]), used_names);

        auto body = applySubscripts(std::make_shared<IdentifierNode>(Identifier{name}), rest, depth + 1);
        auto lambda = std::make_shared<LambdaNode>(std::make_shared<LambdaArgumentsNode>(Names{name}), std::move(body), /*is_operator=*/ true);
        return makeFunction("arrayMap", {std::move(lambda), std::move(array)});
    }
    return array;
}

void flattenProduct(const QueryTreeNodePtr & node, QueryTreeNodes & factors)
{
    const auto * function = node->as<FunctionNode>();
    if (function && function->getFunctionName() == "multiply" && function->getArguments().getNodes().size() == 2)
    {
        for (const auto & argument : function->getArguments().getNodes())
            flattenProduct(argument, factors);
        return;
    }
    factors.push_back(node);
}

class EinsumLambdaRewriter
{
public:
    explicit EinsumLambdaRewriter(const LambdaNode & lambda_)
        : lambda(lambda_), indices(lambda.getArguments().getNames())
    {
        if (indices.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The lambda function of einsum must have at least one index as an argument");
    }

    QueryTreeNodePtr rewrite(const QueryTreeNodes & extra_arguments)
    {
        QueryTreeNodePtr body = lambda.getExpression()->clone();

        std::unordered_set<String> used_names;
        collectIdentifierNames(*body, used_names);
        value_prefix = "x";
        while (std::any_of(used_names.begin(), used_names.end(), [&](const String & name) { return isValueName(name); }))
            value_prefix += "_";

        replaceTensorElements(body);

        for (size_t index = 0; index < indices.size(); ++index)
        {
            bool is_used = std::any_of(elements.begin(), elements.end(),
                [&](const TensorElement & element) { return std::find(element.indices.begin(), element.indices.end(), index) != element.indices.end(); });
            if (!is_used)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Index '{}' of einsum is not used as an array subscript, so its range is unknown", indices[index]);
        }

        for (size_t i = 0; i < extra_arguments.size(); ++i)
        {
            bool is_tensor = std::any_of(arrays.begin(), arrays.end(),
                [&](const QueryTreeNodePtr & array) { return array->isEqual(*extra_arguments[i], {.compare_aliases = false}); });
            if (!is_tensor)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Argument {} of einsum is not an array subscripted by the indices in the lambda function", i + 2);
        }

        findSummationIndices(body);

        /// If the expression is a product of elements, the elements are multiplied by einsum without a lambda function.
        QueryTreeNodes factors;
        flattenProduct(body, factors);
        std::vector<size_t> product_operands;
        for (const auto & factor : factors)
        {
            auto element = getValueNumber(*factor);
            if (!element)
            {
                product_operands.clear();
                break;
            }
            product_operands.push_back(*element);
        }

        bool is_product = !product_operands.empty();
        std::vector<size_t> operands = std::move(product_operands);
        if (!is_product)
            for (size_t i = 0; i < elements.size(); ++i)
                operands.push_back(i);

        /// The letters for the indices: the names of the indices if they are letters, otherwise a, b, c, ...
        std::vector<char> letters;
        bool names_are_letters = std::all_of(indices.begin(), indices.end(), [](const String & name) { return name.size() == 1 && isAlphaASCII(name[0]); });
        for (size_t index = 0; index < indices.size(); ++index)
        {
            if (names_are_letters)
                letters.push_back(indices[index][0]);
            else if (index < 26)
                letters.push_back(static_cast<char>('a' + index));
            else if (index < 52)
                letters.push_back(static_cast<char>('A' + index - 26));
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Too many indices in the lambda function of einsum, the maximum is 52");
        }

        String subscripts;
        QueryTreeNodes arguments;
        arguments.push_back(nullptr);
        for (size_t i = 0; i < operands.size(); ++i)
        {
            if (i)
                subscripts += ',';
            for (size_t index : elements[operands[i]].indices)
                subscripts += letters[index];
        }
        subscripts += "->";
        for (size_t index = 0; index < indices.size(); ++index)
            if (!is_summation[index])
                subscripts += letters[index];
        arguments[0] = std::make_shared<ConstantNode>(Field(subscripts));

        if (!is_product)
        {
            Names value_names;
            for (size_t i = 0; i < elements.size(); ++i)
                value_names.push_back(getValueName(i));
            arguments.push_back(std::make_shared<LambdaNode>(std::make_shared<LambdaArgumentsNode>(value_names), std::move(body), /*is_operator=*/ true));
        }

        for (size_t operand : operands)
            arguments.push_back(elements[operand].tensor);

        return makeFunction("einsum", std::move(arguments));
    }

private:
    const LambdaNode & lambda;
    const Names & indices;

    std::vector<TensorElement> elements;
    /// The arrays of the elements, before the subscripts that are not indices are applied.
    QueryTreeNodes arrays;
    std::vector<bool> is_summation;
    String value_prefix;

    String getValueName(size_t element) const { return value_prefix + std::to_string(element + 1); }

    bool isValueName(const String & name) const
    {
        return name.starts_with(value_prefix) && name.size() > value_prefix.size()
            && std::all_of(name.begin() + value_prefix.size(), name.end(), isNumericASCII);
    }

    std::optional<size_t> getValueNumber(const IQueryTreeNode & node) const
    {
        const auto * identifier = node.as<IdentifierNode>();
        if (!identifier || !identifier->getIdentifier().isShort() || !isValueName(identifier->getIdentifier().getFullName()))
            return {};
        return std::stoull(identifier->getIdentifier().getFullName().substr(value_prefix.size())) - 1;
    }

    std::optional<size_t> getIndex(const IQueryTreeNode & node) const
    {
        const auto * identifier = node.as<IdentifierNode>();
        if (!identifier || !identifier->getIdentifier().isShort())
            return {};
        auto it = std::find(indices.begin(), indices.end(), identifier->getIdentifier().getFullName());
        if (it == indices.end())
            return {};
        return it - indices.begin();
    }

    bool dependsOnIndices(const IQueryTreeNode & node) const
    {
        if (const auto * identifier = node.as<IdentifierNode>())
            return std::find(indices.begin(), indices.end(), identifier->getIdentifier()[0]) != indices.end();

        for (const auto & child : node.getChildren())
            if (child && dependsOnIndices(*child))
                return true;
        return false;
    }

    /// Replaces the elements of tensors with the identifiers of their values.
    void replaceTensorElements(QueryTreeNodePtr & node)
    {
        if (asArrayElement(*node))
        {
            /// The whole chain of subscripts: array[s1][s2]...[sn].
            QueryTreeNodes subscripts;
            QueryTreeNodePtr array = node;
            while (const auto * element = asArrayElement(*array))
            {
                subscripts.push_back(element->getArguments().getNodes()[1]);
                array = element->getArguments().getNodes()[0];
            }
            std::reverse(subscripts.begin(), subscripts.end());

            bool has_indices = std::any_of(subscripts.begin(), subscripts.end(), [&](const QueryTreeNodePtr & subscript) { return dependsOnIndices(*subscript); });
            if (has_indices)
            {
                node = replaceTensorElement(array, subscripts);
                return;
            }
        }

        if (getIndex(*node))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Index '{}' of einsum can be used only as an array subscript", node->as<IdentifierNode &>().getIdentifier().getFullName());

        if (const auto * nested_lambda = node->as<LambdaNode>())
        {
            for (const auto & name : nested_lambda->getArguments().getNames())
                if (std::find(indices.begin(), indices.end(), name) != indices.end())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "A lambda function inside einsum cannot have an argument with the name of the index '{}'", name);
        }

        for (auto & child : node->getChildren())
            if (child)
                replaceTensorElements(child);
    }

    QueryTreeNodePtr replaceTensorElement(const QueryTreeNodePtr & array, const QueryTreeNodes & subscripts)
    {
        if (dependsOnIndices(*array))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "The array {} in einsum depends on the indices, but they can be used only as array subscripts", array->formatASTForErrorMessage());

        TensorElement element;
        QueryTreeNodes fixed_subscripts;
        for (const auto & subscript : subscripts)
        {
            if (auto index = getIndex(*subscript))
            {
                element.indices.push_back(*index);
                fixed_subscripts.push_back(nullptr);
            }
            else if (dependsOnIndices(*subscript))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "A subscript of an array in einsum must be an index or an expression that does not depend on the indices, got {}",
                    subscript->formatASTForErrorMessage());
            else
                fixed_subscripts.push_back(subscript);
        }
        element.tensor = applySubscripts(array->clone(), fixed_subscripts);

        /// The same element is the same value of the lambda function.
        for (size_t i = 0; i < elements.size(); ++i)
            if (elements[i].indices == element.indices && elements[i].tensor->isEqual(*element.tensor, {.compare_aliases = false}))
                return std::make_shared<IdentifierNode>(Identifier{getValueName(i)});

        elements.push_back(std::move(element));
        arrays.push_back(array);
        return std::make_shared<IdentifierNode>(Identifier{getValueName(elements.size() - 1)});
    }

    /// The indices of the elements in the expression.
    void collectIndices(const IQueryTreeNode & node, std::vector<bool> & used) const
    {
        if (auto element = getValueNumber(node))
        {
            for (size_t index : elements[*element].indices)
                used[index] = true;
            return;
        }
        for (const auto & child : node.getChildren())
            if (child)
                collectIndices(*child, used);
    }

    /// An index is a summation index if it is used in two or more factors of some product.
    void findSummationIndices(const QueryTreeNodePtr & body)
    {
        is_summation.assign(indices.size(), false);
        findSummationIndicesImpl(body);
    }

    void findSummationIndicesImpl(const QueryTreeNodePtr & node)
    {
        QueryTreeNodes factors;
        flattenProduct(node, factors);
        if (factors.size() > 1)
        {
            std::vector<size_t> number_of_factors(indices.size(), 0);
            for (const auto & factor : factors)
            {
                std::vector<bool> used(indices.size(), false);
                collectIndices(*factor, used);
                for (size_t index = 0; index < indices.size(); ++index)
                    number_of_factors[index] += used[index];
            }
            for (size_t index = 0; index < indices.size(); ++index)
                if (number_of_factors[index] >= 2)
                    is_summation[index] = true;

            for (const auto & factor : factors)
                findSummationIndicesImpl(factor);
            return;
        }

        for (const auto & child : node->getChildren())
            if (child)
                findSummationIndicesImpl(child);
    }
};

}

QueryTreeNodePtr rewriteEinsumLambda(const FunctionNode & function_node)
{
    const auto & arguments = function_node.getArguments().getNodes();
    const auto & lambda = arguments.at(0)->as<const LambdaNode &>();

    QueryTreeNodes extra_arguments(arguments.begin() + 1, arguments.end());
    auto res = EinsumLambdaRewriter(lambda).rewrite(extra_arguments);
    res->setAlias(function_node.getAlias());
    res->setOriginalAST(function_node.getOriginalAST());
    return res;
}

}
