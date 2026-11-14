#include <Analyzer/Resolve/EinsumLambdaRewrite.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/IdentifierNode.h>
#include <Analyzer/LambdaNode.h>
#include <Analyzer/ListNode.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/checkStackSize.h>

#include <functional>
#include <set>
#include <span>
#include <unordered_map>
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
    checkStackSize();

    /// An alias is a name in the scope of the lambda function as well, a generated name must not clash with it.
    if (node.hasAlias())
        names.insert(node.getAlias());

    if (const auto * identifier = node.as<IdentifierNode>())
    {
        names.insert(identifier->getIdentifier().getFullName());
        for (const auto & part : identifier->getIdentifier().getParts())
            names.insert(part);
    }
    if (const auto * column = node.as<ColumnNode>())
        names.insert(column->getColumnName());
    if (const auto * lambda = node.as<LambdaNode>())
        for (const auto & name : lambda->getArguments().getNames())
            names.insert(name);

    for (const auto & child : node.getChildren())
        if (child)
            collectIdentifierNames(*child, names);
}

/// Whether an alias can bind a name to a function: the analyzer looks up the names of functions among the aliases of lambda
/// functions and of identifiers, because an identifier, like `f` in `f AS g`, can refer to a lambda function.
bool isFunctionAliasExpression(const IQueryTreeNode & node)
{
    return node.getNodeType() == QueryTreeNodeType::LAMBDA || node.getNodeType() == QueryTreeNodeType::IDENTIFIER;
}

/// Whether the name can be bound to a function by an alias anywhere in the expression,
/// like `((x, y) -> x + y) AS arrayElement` or `f AS arrayElement`.
bool hasFunctionAlias(const IQueryTreeNode & node, const String & name)
{
    checkStackSize();

    if (node.getAlias() == name && isFunctionAliasExpression(node))
        return true;
    for (const auto & child : node.getChildren())
        if (child && hasFunctionAlias(*child, name))
            return true;
    return false;
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

/// Collects the names in the lambda functions bound in the query outside of the lambda function of einsum, and in the SQL user
/// defined functions, that are called in the expression, also transitively: their expressions are resolved where they are called,
/// so a generated parameter with the same name as a free name in them would capture it.
void collectNamesOfCalledFunctions(
    const IQueryTreeNode & node,
    const ScopedFunctionLookup & get_called_lambda,
    std::unordered_set<String> & names,
    std::unordered_set<String> & visited_functions)
{
    checkStackSize();

    if (const auto * function = node.as<FunctionNode>(); function && visited_functions.insert(function->getFunctionName()).second)
    {
        if (auto called_lambda = get_called_lambda(function->getFunctionName()))
        {
            collectIdentifierNames(*called_lambda, names);
            collectNamesOfCalledFunctions(*called_lambda, get_called_lambda, names, visited_functions);
        }
    }

    for (const auto & child : node.getChildren())
        if (child)
            collectNamesOfCalledFunctions(*child, get_called_lambda, names, visited_functions);
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
    checkStackSize();

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

/// Whether the subscript can have a free name that is not visible in its syntax: a call of a function can be a call
/// of a lambda function, like `f` in `WITH y -> y + x AS f`, whose expression is resolved where it is called.
bool containsFunction(const IQueryTreeNode & node)
{
    checkStackSize();

    switch (node.getNodeType())
    {
        case QueryTreeNodeType::FUNCTION:
        case QueryTreeNodeType::LAMBDA:
        case QueryTreeNodeType::QUERY:
        case QueryTreeNodeType::UNION:
            return true;
        default:
            break;
    }
    for (const auto & child : node.getChildren())
        if (child && containsFunction(*child))
            return true;
    return false;
}

/** Like `applySubscripts`, but the subscripts after the first `nullptr` that are not constants are calculated outside
  * of the generated lambda functions, if one of them contains a function, so that a generated parameter cannot capture a free name:
  *   arr[][f(0)] -> arrayMap((b, s1) -> arrayMap(x -> x[s1], b), [arr], [f(0)])[1]
  * Then the generated lambda functions contain only the generated names.
  */
QueryTreeNodePtr applySubscriptsWithoutCapture(QueryTreeNodePtr array, std::span<const QueryTreeNodePtr> subscripts)
{
    auto first_empty = std::find(subscripts.begin(), subscripts.end(), nullptr);
    bool has_function = std::any_of(first_empty, subscripts.end(),
        [](const QueryTreeNodePtr & subscript) { return subscript && containsFunction(*subscript); });
    if (!has_function)
        return applySubscripts(std::move(array), subscripts);

    array = applySubscripts(std::move(array), subscripts.first(first_empty - subscripts.begin()));

    Names names{"b"};
    QueryTreeNodes arguments{nullptr, makeFunction("array", {std::move(array)})};
    QueryTreeNodes rest;
    for (auto it = first_empty; it != subscripts.end(); ++it)
    {
        if (!*it || (*it)->getNodeType() == QueryTreeNodeType::CONSTANT)
        {
            rest.push_back(*it);
            continue;
        }
        names.push_back("s" + std::to_string(names.size()));
        arguments.push_back(makeFunction("array", {(*it)->clone()}));
        rest.push_back(std::make_shared<IdentifierNode>(Identifier{names.back()}));
    }

    auto body = applySubscripts(std::make_shared<IdentifierNode>(Identifier{"b"}), rest);
    arguments[0] = std::make_shared<LambdaNode>(std::make_shared<LambdaArgumentsNode>(names), std::move(body), /*is_operator=*/ true);
    return makeFunction("arrayElement", {makeFunction("arrayMap", std::move(arguments)), std::make_shared<ConstantNode>(Field(UInt64(1)))});
}

bool isSubquery(const IQueryTreeNode & node)
{
    return node.getNodeType() == QueryTreeNodeType::QUERY || node.getNodeType() == QueryTreeNodeType::UNION;
}

/// The aliases of a scope of the lambda function of einsum or of a lambda function inside it.
/// An alias defined inside a nested lambda function is visible only there, and the parameters of a nested lambda function
/// hide the aliases with the same names from the outer scopes.
struct AliasScope
{
    /// The nodes are owned, because the elements of tensors are replaced in the expression while the scope is used.
    std::unordered_map<String, QueryTreeNodePtr> aliases;
    std::unordered_set<String> parameters;
    const AliasScope * parent = nullptr;
};

/// Collects the aliases of the scope, without the nested lambda functions and subqueries, which have their own scopes.
void collectScopeAliases(const QueryTreeNodePtr & node, AliasScope & scope)
{
    checkStackSize();

    if (node->hasAlias())
        scope.aliases.emplace(node->getAlias(), node);
    if (node->getNodeType() == QueryTreeNodeType::LAMBDA || isSubquery(*node))
        return;
    for (const auto & child : node->getChildren())
        if (child)
            collectScopeAliases(child, scope);
}

/// The scope of the expression of a nested lambda function inside `parent`.
AliasScope makeLambdaScope(const LambdaNode & lambda, const AliasScope & parent)
{
    AliasScope scope;
    scope.parent = &parent;
    for (const auto & name : lambda.getArguments().getNames())
        scope.parameters.insert(name);
    collectScopeAliases(lambda.getExpression(), scope);
    return scope;
}

/// The scope of a subquery inside `parent`: the aliases inside the subquery are visible only there.
AliasScope makeSubqueryScope(const IQueryTreeNode & subquery, const AliasScope & parent)
{
    AliasScope scope;
    scope.parent = &parent;
    for (const auto & child : subquery.getChildren())
        if (child)
            collectScopeAliases(child, scope);
    return scope;
}

/// The expression of the alias visible in `scope` and the scope where it is defined, or nullptr if the name is not an alias.
std::pair<const IQueryTreeNode *, const AliasScope *> findAlias(const String & name, const AliasScope & scope)
{
    for (const AliasScope * current = &scope; current; current = current->parent)
    {
        if (current->parameters.contains(name))
            return {nullptr, nullptr};
        if (auto it = current->aliases.find(name); it != current->aliases.end())
            return {it->second.get(), current};
    }
    return {nullptr, nullptr};
}

/// What a call of a function with a name refers to in a scope inside the lambda function of einsum.
struct FunctionBinding
{
    /// The name refers to a lambda function, not to a builtin function.
    bool is_lambda = false;
    /// The name is bound by an alias inside the lambda function of einsum, so it means something else outside of it.
    bool is_local = false;
    /// The lambda function inside the lambda function of einsum and its scope, if the name refers to it.
    const IQueryTreeNode * lambda = nullptr;
    const AliasScope * lambda_scope = nullptr;
};

/** Looks up the name of a function like the analyzer does: among the aliases of lambda functions and of identifiers
  * (see `isFunctionAliasExpression`), following the chains of aliases like `f AS g, g AS multiply`, and then among the functions
  * bound in the query outside of the lambda function of einsum (`is_scoped_function`). A scalar alias with the name of a function
  * does not hide the function.
  */
FunctionBinding resolveFunctionName(
    String name, const AliasScope & scope, const ScopedFunctionLookup & is_scoped_function)
{
    FunctionBinding binding;
    std::unordered_set<const IQueryTreeNode *> visited;
    const AliasScope * lookup_scope = &scope;
    while (true)
    {
        const IQueryTreeNode * expression = nullptr;
        const AliasScope * expression_scope = nullptr;
        for (const AliasScope * current = lookup_scope; current && !expression; current = current->parent)
        {
            /// A parameter of a nested lambda function can be a lambda function, like `f` in `invoke(f -> f(0), x -> [x])`.
            if (current->parameters.contains(name))
            {
                binding.is_lambda = true;
                binding.is_local = true;
                return binding;
            }
            if (auto it = current->aliases.find(name); it != current->aliases.end() && isFunctionAliasExpression(*it->second))
            {
                expression = it->second.get();
                expression_scope = current;
            }
        }

        if (!expression)
        {
            binding.is_lambda = is_scoped_function(name) != nullptr;
            return binding;
        }

        binding.is_local = true;
        if (expression->getNodeType() == QueryTreeNodeType::LAMBDA)
        {
            binding.is_lambda = true;
            binding.lambda = expression;
            binding.lambda_scope = expression_scope;
            return binding;
        }

        /// An alias of an identifier, the identifier is looked up in the scope of the alias.
        const auto & identifier = expression->as<const IdentifierNode &>().getIdentifier();
        if (!identifier.isShort() || !visited.insert(expression).second)
            return binding;
        name = identifier.getFullName();
        lookup_scope = expression_scope;
    }
}

/// Whether the name is a parameter of a nested lambda function or an alias visible in `scope`.
/// The aliases of the scopes are only looked up by their names here, the expressions are not used.
bool isLocalName(const String & name, const AliasScope & scope)
{
    for (const AliasScope * current = &scope; current; current = current->parent)
        if (current->parameters.contains(name) || current->aliases.contains(name))
            return true;
    return false;
}

/** A name in the expression that is bound inside the lambda function of einsum (see `isLocalName`), or an empty string.
  * The parameters of the lambda functions inside the expression itself, in `inner_parameters`, are bound in the expression.
  */
String findLocalName(
    const IQueryTreeNode & node,
    const AliasScope & scope,
    std::unordered_set<String> & inner_parameters,
    const ScopedFunctionLookup & is_scoped_function)
{
    checkStackSize();

    /// A function can be bound to a lambda function by an alias, like `f` in `(x -> [x]) AS f`, which is a local name as well.
    if (const auto * function = node.as<FunctionNode>())
    {
        const auto & name = function->getFunctionName();
        if (!inner_parameters.contains(name) && resolveFunctionName(name, scope, is_scoped_function).is_local)
            return name;
    }

    if (const auto * identifier = node.as<IdentifierNode>())
    {
        const auto & name = identifier->getIdentifier()[0];
        if (!inner_parameters.contains(name) && isLocalName(name, scope))
            return name;
        return {};
    }

    if (const auto * lambda = node.as<LambdaNode>())
    {
        Names added;
        for (const auto & name : lambda->getArguments().getNames())
            if (inner_parameters.insert(name).second)
                added.push_back(name);
        String res = findLocalName(*lambda->getExpression(), scope, inner_parameters, is_scoped_function);
        for (const auto & name : added)
            inner_parameters.erase(name);
        return res;
    }

    for (const auto & child : node.getChildren())
    {
        if (!child)
            continue;
        if (String res = findLocalName(*child, scope, inner_parameters, is_scoped_function); !res.empty())
            return res;
    }
    return {};
}

class EinsumLambdaRewriter
{
public:
    EinsumLambdaRewriter(
        const LambdaNode & lambda_, const ScopedFunctionLookup & is_scoped_function_, const ScopedFunctionLookup & get_called_lambda_)
        : lambda(lambda_), indices(lambda.getArguments().getNames()), is_scoped_function(is_scoped_function_)
        , get_called_lambda(get_called_lambda_)
    {
        if (indices.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The lambda function of einsum must have at least one index as an argument");

        /// The names of the functions in the expression can be bound to lambda functions in the query, like
        /// `WITH (x, y) -> x + y AS multiply`, or to the aliases of lambda functions in the expression.
        /// Then they are not the builtin functions. The aliases inside the expression are checked in their scopes, see `isMultiply`.
        if (is_scoped_function("arrayElement") || hasFunctionAlias(*lambda.getExpression(), "arrayElement"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "The lambda form of einsum is not supported when the name 'arrayElement' is bound to another function in the query");
    }

    QueryTreeNodePtr rewrite(const QueryTreeNodes & extra_arguments)
    {
        QueryTreeNodePtr body = lambda.getExpression()->clone();

        std::unordered_set<String> used_names;
        collectIdentifierNames(*body, used_names);
        {
            std::unordered_set<String> visited_functions;
            collectNamesOfCalledFunctions(*body, get_called_lambda, used_names, visited_functions);
        }
        value_prefix = "x";
        while (std::any_of(used_names.begin(), used_names.end(), [&](const String & name) { return isValueName(name); }))
            value_prefix += "_";

        {
            /// The names of the aliases are used before the elements are replaced, the expressions of the aliases are not.
            AliasScope scope;
            collectScopeAliases(body, scope);
            replaceTensorElements(body, scope);
        }

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

        AliasScope body_scope;
        collectScopeAliases(body, body_scope);
        findSummationIndices(body, body_scope);

        /// If the expression is a product of elements, the elements are multiplied by einsum without a lambda function.
        QueryTreeNodes factors;
        flattenProduct(body, body_scope, factors);
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
    /// Whether the name is bound to a lambda function in the query outside of the lambda function of einsum.
    const ScopedFunctionLookup & is_scoped_function;
    /// The lambda function that is called by the name: bound in the query or a SQL user defined function.
    const ScopedFunctionLookup & get_called_lambda;

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
        checkStackSize();

        if (const auto * identifier = node.as<IdentifierNode>())
            return std::find(indices.begin(), indices.end(), identifier->getIdentifier()[0]) != indices.end();

        for (const auto & child : node.getChildren())
            if (child && dependsOnIndices(*child))
                return true;
        return false;
    }

    /// Replaces the elements of tensors with the identifiers of their values.
    /// `scope` has the names of the aliases and of the parameters of the nested lambda functions visible at `node`.
    void replaceTensorElements(QueryTreeNodePtr & node, const AliasScope & scope)
    {
        checkStackSize();

        if (asArrayElement(*node))
        {
            /// The whole chain of subscripts: array[s1][s2]...[sn].
            QueryTreeNodes subscripts;
            QueryTreeNodes intermediate_elements;
            QueryTreeNodePtr array = node;
            while (const auto * element = asArrayElement(*array))
            {
                if (array != node)
                    intermediate_elements.push_back(array);
                subscripts.push_back(element->getArguments().getNodes()[1]);
                array = element->getArguments().getNodes()[0];
            }
            std::reverse(subscripts.begin(), subscripts.end());

            bool has_indices = std::any_of(subscripts.begin(), subscripts.end(), [&](const QueryTreeNodePtr & subscript) { return dependsOnIndices(*subscript); });
            if (has_indices)
            {
                /// The array and the subscripts that are not indices are moved out of the lambda function to the arguments
                /// of einsum, so an alias inside them, like in `(a[1] AS v)[i] + v[1]`, would lose its definition in the lambda.
                for (const auto & intermediate_element : intermediate_elements)
                    if (intermediate_element->hasAlias())
                        throwAliasInsideTensorElement(intermediate_element->getAlias(), *node);
                checkNoAliases(*array, *node);
                for (const auto & subscript : subscripts)
                    checkNoAliases(*subscript, *node);

                /// For the same reason, the array and the subscripts that are not indices must not depend on the parameters
                /// of the nested lambda functions, like `a` in `arrayMap(a -> a[i], arrays)`, and on the aliases inside
                /// the lambda function: in the arguments of einsum, these names would refer to something else.
                std::unordered_set<String> inner_parameters;
                if (String name = findLocalName(*array, scope, inner_parameters, is_scoped_function); !name.empty())
                    throwLocalNameInsideTensorElement(name, *node);
                for (const auto & subscript : subscripts)
                    if (!getIndex(*subscript))
                        if (String name = findLocalName(*subscript, scope, inner_parameters, is_scoped_function); !name.empty())
                            throwLocalNameInsideTensorElement(name, *node);

                /// The alias of the element, like in `(a[i] AS v) + v`, is the alias of its value now.
                String alias = node->getAlias();
                node = replaceTensorElement(array, subscripts);
                if (!alias.empty())
                    node->setAlias(alias);
                return;
            }
        }

        if (getIndex(*node))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Index '{}' of einsum can be used only as an array subscript", node->as<IdentifierNode &>().getIdentifier().getFullName());

        if (auto * nested_lambda = node->as<LambdaNode>())
        {
            for (const auto & name : nested_lambda->getArguments().getNames())
                if (std::find(indices.begin(), indices.end(), name) != indices.end())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "A lambda function inside einsum cannot have an argument with the name of the index '{}'", name);

            auto lambda_scope = makeLambdaScope(*nested_lambda, scope);
            replaceTensorElements(nested_lambda->getExpression(), lambda_scope);
            return;
        }

        if (isSubquery(*node))
        {
            auto subquery_scope = makeSubqueryScope(*node, scope);
            for (auto & child : node->getChildren())
                if (child)
                    replaceTensorElements(child, subquery_scope);
            return;
        }

        for (auto & child : node->getChildren())
            if (child)
                replaceTensorElements(child, scope);
    }

    [[noreturn]] static void throwLocalNameInsideTensorElement(const String & name, const IQueryTreeNode & element)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "The element {} of a tensor in einsum depends on '{}', which is an alias or a parameter of a lambda function inside einsum. "
            "This is not supported, because the array and its subscripts that are not indices are computed outside of the lambda function",
            element.formatASTForErrorMessage(), name);
    }

    [[noreturn]] static void throwAliasInsideTensorElement(const String & alias, const IQueryTreeNode & element)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "The alias '{}' inside the element {} of a tensor in einsum is not supported, "
            "because the array and its subscripts are computed outside of the lambda function. Use the expression instead of the alias",
            alias, element.formatASTForErrorMessage());
    }

    static void checkNoAliases(const IQueryTreeNode & node, const IQueryTreeNode & element)
    {
        checkStackSize();

        if (node.hasAlias())
            throwAliasInsideTensorElement(node.getAlias(), element);
        for (const auto & child : node.getChildren())
            if (child)
                checkNoAliases(*child, element);
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
        element.tensor = applySubscriptsWithoutCapture(array->clone(), fixed_subscripts);

        /// The same element is the same value of the lambda function.
        for (size_t i = 0; i < elements.size(); ++i)
            if (elements[i].indices == element.indices && elements[i].tensor->isEqual(*element.tensor, {.compare_aliases = false}))
                return std::make_shared<IdentifierNode>(Identifier{getValueName(i)});

        elements.push_back(std::move(element));
        arrays.push_back(array);
        return std::make_shared<IdentifierNode>(Identifier{getValueName(elements.size() - 1)});
    }

    /// Whether the node is a call of the builtin function `multiply` in `scope`.
    bool isMultiply(const IQueryTreeNode & node, const AliasScope & scope) const
    {
        const auto * function = node.as<FunctionNode>();
        return function && function->getFunctionName() == "multiply" && function->getArguments().getNodes().size() == 2
            && !resolveFunctionName("multiply", scope, is_scoped_function).is_lambda;
    }

    void flattenProduct(const QueryTreeNodePtr & node, const AliasScope & scope, QueryTreeNodes & factors) const
    {
        checkStackSize();

        if (isMultiply(*node, scope))
        {
            for (const auto & argument : node->as<const FunctionNode &>().getArguments().getNodes())
                flattenProduct(argument, scope, factors);
            return;
        }
        factors.push_back(node);
    }

    /// The indices of the elements in the expression, also through the references to aliases visible in `scope`.
    /// `visited_aliases` protects from the cycles of aliases.
    void collectIndices(
        const IQueryTreeNode & node, const AliasScope & scope, std::vector<bool> & used, std::set<const IQueryTreeNode *> & visited_aliases) const
    {
        checkStackSize();

        if (auto element = getValueNumber(node))
        {
            for (size_t index : elements[*element].indices)
                used[index] = true;
            return;
        }

        if (const auto * identifier = node.as<IdentifierNode>(); identifier && identifier->getIdentifier().isShort())
        {
            auto [alias_expression, alias_scope] = findAlias(identifier->getIdentifier().getFullName(), scope);
            if (alias_expression && alias_expression != &node && visited_aliases.insert(alias_expression).second)
                collectIndices(*alias_expression, *alias_scope, used, visited_aliases);
            return;
        }

        if (const auto * nested_lambda = node.as<LambdaNode>())
        {
            auto lambda_scope = makeLambdaScope(*nested_lambda, scope);
            collectIndices(*nested_lambda->getExpression(), lambda_scope, used, visited_aliases);
            return;
        }

        if (isSubquery(node))
        {
            auto subquery_scope = makeSubqueryScope(node, scope);
            for (const auto & child : node.getChildren())
                if (child)
                    collectIndices(*child, subquery_scope, used, visited_aliases);
            return;
        }

        /// A call of a lambda function bound by an alias, like `f(0)` with `(x -> a[i]) AS f`, uses the indices of its expression.
        if (const auto * function = node.as<FunctionNode>())
        {
            auto binding = resolveFunctionName(function->getFunctionName(), scope, is_scoped_function);
            if (binding.lambda && visited_aliases.insert(binding.lambda).second)
                collectIndices(*binding.lambda, *binding.lambda_scope, used, visited_aliases);
        }

        for (const auto & child : node.getChildren())
            if (child)
                collectIndices(*child, scope, used, visited_aliases);
    }

    /// An index is a summation index if it is used in two or more factors of some product.
    void findSummationIndices(const QueryTreeNodePtr & body, const AliasScope & scope)
    {
        is_summation.assign(indices.size(), false);
        findSummationIndicesImpl(body, scope);
    }

    void findSummationIndicesImpl(const QueryTreeNodePtr & node, const AliasScope & scope)
    {
        checkStackSize();

        if (const auto * nested_lambda = node->as<LambdaNode>())
        {
            auto lambda_scope = makeLambdaScope(*nested_lambda, scope);
            findSummationIndicesImpl(nested_lambda->getExpression(), lambda_scope);
            return;
        }

        if (isSubquery(*node))
        {
            auto subquery_scope = makeSubqueryScope(*node, scope);
            for (const auto & child : node->getChildren())
                if (child)
                    findSummationIndicesImpl(child, subquery_scope);
            return;
        }

        QueryTreeNodes factors;
        flattenProduct(node, scope, factors);
        if (factors.size() > 1)
        {
            std::vector<size_t> number_of_factors(indices.size(), 0);
            for (const auto & factor : factors)
            {
                std::vector<bool> used(indices.size(), false);
                std::set<const IQueryTreeNode *> visited_aliases;
                collectIndices(*factor, scope, used, visited_aliases);
                for (size_t index = 0; index < indices.size(); ++index)
                    number_of_factors[index] += used[index];
            }
            for (size_t index = 0; index < indices.size(); ++index)
                if (number_of_factors[index] >= 2)
                    is_summation[index] = true;

            for (const auto & factor : factors)
                findSummationIndicesImpl(factor, scope);
            return;
        }

        for (const auto & child : node->getChildren())
            if (child)
                findSummationIndicesImpl(child, scope);
    }
};

}

QueryTreeNodePtr rewriteEinsumLambda(
    const FunctionNode & function_node, const ScopedFunctionLookup & is_scoped_function, const ScopedFunctionLookup & get_called_lambda)
{
    const auto & arguments = function_node.getArguments().getNodes();
    const auto & lambda = arguments.at(0)->as<const LambdaNode &>();

    QueryTreeNodes extra_arguments(arguments.begin() + 1, arguments.end());
    auto res = EinsumLambdaRewriter(lambda, is_scoped_function, get_called_lambda).rewrite(extra_arguments);
    res->setAlias(function_node.getAlias());
    res->setOriginalAST(function_node.getOriginalAST());
    return res;
}

}
