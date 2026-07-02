#include <Interpreters/pullUpTupleElementDefaults.h>

#include <Core/Names.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/ASTTupleDataType.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

/// Helpers for pulling up DEFAULT expressions written inside Tuple data types to the column level.
/// For example, the column `c Tuple(a UInt8, s String DEFAULT 'Hello')` is normalized to
/// `c Tuple(a UInt8, s String) DEFAULT tuple(defaultValueOfTypeName('UInt8'), 'Hello')`.
/// See https://github.com/ClickHouse/ClickHouse/issues/2797.

/// Build `defaultValueOfTypeName('<type>')` for the (already stripped) element type, used to fill
/// positions of a tuple that do not have an explicit DEFAULT.
ASTPtr makeDefaultValueOfType(const IAST & type)
{
    return makeASTFunction("defaultValueOfTypeName", make_intrusive<ASTLiteral>(type.formatForLogging()));
}

/// Build `tuple(e0, e1, ...)` where each element is either its explicit default expression or the
/// default value of its type.
ASTPtr makeTupleDefault(const ASTs & element_types, const ASTs & element_defaults)
{
    auto arguments = make_intrusive<ASTExpressionList>();
    arguments->children.reserve(element_defaults.size());
    for (size_t i = 0; i < element_defaults.size(); ++i)
    {
        if (element_defaults[i])
            arguments->children.push_back(element_defaults[i]);
        else
            arguments->children.push_back(makeDefaultValueOfType(*element_types[i]));
    }

    auto function = make_intrusive<ASTFunction>();
    function->name = "tuple";
    function->arguments = arguments;
    function->children.push_back(arguments);
    return function;
}

/// Collect the names of identifiers referenced by an expression (for the ambiguity check below).
/// Lambda parameters (e.g. `x` in `arrayMap(x -> x + 1, arr)`) are scoped local variables, not
/// references to columns or tuple elements, so they are skipped while free identifiers from the
/// lambda body are still collected. `bound` holds the names currently bound by enclosing lambdas.
void collectReferencedIdentifiers(const IAST & ast, NameSet & names, NameSet & bound)
{
    if (const auto * function = ast.as<ASTFunction>(); function && function->name == "lambda"
        && function->arguments && function->arguments->children.size() == 2)
    {
        /// The first argument of a lambda is always a `tuple(...)` of parameter identifiers; only
        /// its second argument (the body) can reference outer names.
        Names added;
        if (const auto * params = function->arguments->children[0]->as<ASTFunction>();
            params && params->name == "tuple" && params->arguments)
        {
            for (const auto & param : params->arguments->children)
                if (const auto * identifier = param->as<ASTIdentifier>())
                    if (bound.insert(identifier->name()).second)
                        added.push_back(identifier->name());
        }

        collectReferencedIdentifiers(*function->arguments->children[1], names, bound);

        for (const auto & name : added)
            bound.erase(name);
        return;
    }

    if (const auto * identifier = ast.as<ASTIdentifier>())
    {
        /// The root of a compound identifier (`x` in `x.y`) determines what it refers to: if it is a
        /// bound lambda parameter, the whole reference is local and must not be collected.
        const String & root = identifier->name_parts.empty() ? identifier->name() : identifier->name_parts.front();
        if (!bound.contains(root))
        {
            names.insert(identifier->name());
            if (!identifier->name_parts.empty())
                names.insert(identifier->name_parts.front());
        }
        return;
    }

    for (const auto & child : ast.children)
        collectReferencedIdentifiers(*child, names, bound);
}

/// Strip the DEFAULT expression from a name-type pair (turning it back into a plain element).
void stripDefaultFromNameTypePair(ASTNameTypePair & pair)
{
    pair.default_expression = nullptr;
    pair.children.clear();
    if (pair.type)
        pair.children.push_back(pair.type);
}

/// Recursively walk a data type AST, building a column-level default expression from any DEFAULT
/// expressions reachable through a chain of Tuples and stripping them from the type. Returns the
/// default-value expression if the type (recursively) contains DEFAULTs, otherwise nullptr.
///
/// A DEFAULT inside a non-Tuple wrapper (Array, Map, Nested, ...) cannot be represented as a static
/// column default, so it is rejected with NOT_IMPLEMENTED.
///
/// All tuple/nested element names are collected into `element_names`, and the user's explicit
/// default expressions into `explicit_defaults`, for the ambiguity check performed by the caller.
ASTPtr buildAndStripTupleDefaults(IAST & type, NameSet & element_names, ASTs & explicit_defaults)
{
    /// Named/unnamed tuple parsed via the fast path (ASTTupleDataType). It never carries DEFAULTs
    /// directly (those force the generic parser path), but its element types might contain them.
    if (auto * tuple = type.as<ASTTupleDataType>())
    {
        for (const auto & name : tuple->element_names)
            if (!name.empty())
                element_names.insert(name);

        const auto arguments = tuple->getArguments();
        if (!arguments)
            return nullptr;

        bool has_default = false;
        ASTs element_defaults(arguments->children.size());
        for (size_t i = 0; i < arguments->children.size(); ++i)
        {
            element_defaults[i] = buildAndStripTupleDefaults(*arguments->children[i], element_names, explicit_defaults);
            has_default |= element_defaults[i] != nullptr;
        }

        if (!has_default)
            return nullptr;
        return makeTupleDefault(arguments->children, element_defaults);
    }

    auto * data_type = type.as<ASTDataType>();
    if (!data_type)
        return nullptr;

    const auto arguments = data_type->getArguments();

    /// Generic Tuple parsed via the fallback path: elements are ASTNameTypePair (possibly with a
    /// DEFAULT) or bare types.
    if (data_type->name == "Tuple")
    {
        if (!arguments)
            return nullptr;

        bool has_default = false;
        ASTs element_types(arguments->children.size());
        ASTs element_defaults(arguments->children.size());
        for (size_t i = 0; i < arguments->children.size(); ++i)
        {
            ASTPtr & child = arguments->children[i];
            if (auto * pair = child->as<ASTNameTypePair>())
            {
                if (!pair->name.empty())
                    element_names.insert(pair->name);

                ASTPtr explicit_default = pair->default_expression;
                ASTPtr nested_default = buildAndStripTupleDefaults(*pair->type, element_names, explicit_defaults);

                if (explicit_default && nested_default)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Tuple element '{}' has both a DEFAULT expression and DEFAULT expressions inside its type",
                        pair->name);

                if (explicit_default)
                {
                    explicit_defaults.push_back(explicit_default);
                    stripDefaultFromNameTypePair(*pair);
                }

                element_types[i] = pair->type;
                element_defaults[i] = explicit_default ? explicit_default : nested_default;
            }
            else
            {
                element_types[i] = child;
                element_defaults[i] = buildAndStripTupleDefaults(*child, element_names, explicit_defaults);
            }
            has_default |= element_defaults[i] != nullptr;
        }

        if (!has_default)
            return nullptr;
        return makeTupleDefault(element_types, element_defaults);
    }

    /// Nested: collect element names and reject any DEFAULT (it is Array(Tuple(...)) and a scalar
    /// element default cannot be a static array column default).
    if (data_type->name == "Nested")
    {
        if (arguments)
        {
            for (const auto & child : arguments->children)
            {
                auto * pair = child->as<ASTNameTypePair>();
                if (!pair)
                    continue;
                if (!pair->name.empty())
                    element_names.insert(pair->name);

                NameSet inner_names;
                ASTs inner_defaults;
                if (pair->default_expression || buildAndStripTupleDefaults(*pair->type, inner_names, inner_defaults))
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "DEFAULT expressions inside Nested are not supported (found for element '{}'). "
                        "Only Tuple supports DEFAULT expressions for its elements.",
                        pair->name);
            }
        }
        return nullptr;
    }

    /// Nullable is a transparent wrapper: a DEFAULT inside Nullable(Tuple(...)) (enabled by the
    /// enable_nullable_tuple_type setting) is representable as the same column-level tuple(...)
    /// default, which is then cast to the nullable tuple type. If the setting is off, the
    /// Nullable(Tuple) type itself is rejected later during type validation.
    if (data_type->name == "Nullable")
    {
        if (!arguments || arguments->children.size() != 1)
            return nullptr;
        return buildAndStripTupleDefaults(*arguments->children[0], element_names, explicit_defaults);
    }

    /// Any other composite type (Array, Map, LowCardinality, ...): a DEFAULT inside is not
    /// representable as a static column default.
    if (arguments)
    {
        for (const auto & child : arguments->children)
        {
            NameSet inner_names;
            ASTs inner_defaults;
            if (buildAndStripTupleDefaults(*child, inner_names, inner_defaults))
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "DEFAULT expressions inside {} are not supported; they are only supported inside Tuple",
                    data_type->name);
        }
    }
    return nullptr;
}

}

void pullUpTupleElementDefaults(ASTColumnDeclaration & col_decl)
{
    auto type = col_decl.getType();
    if (!type)
        return;

    NameSet element_names;
    ASTs explicit_defaults;
    ASTPtr built_default = buildAndStripTupleDefaults(*type, element_names, explicit_defaults);
    if (!built_default)
        return;

    if (col_decl.default_specifier != ColumnDefaultSpecifier::Empty || col_decl.getDefaultExpression())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Column '{}' cannot have both a column-level default and DEFAULT expressions inside its data type",
            col_decl.name);

    /// A DEFAULT expression may reference other columns, but not other elements of the same
    /// tuple/nested. If a referenced name collides with an element name it is ambiguous, so reject.
    NameSet referenced;
    for (const auto & expression : explicit_defaults)
    {
        NameSet bound;
        collectReferencedIdentifiers(*expression, referenced, bound);
    }
    for (const auto & name : referenced)
        if (element_names.contains(name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "DEFAULT expression inside the data type of column '{}' references '{}', which is a tuple/nested "
                "element name. Default expressions cannot reference other elements of the same tuple/nested, and a "
                "reference that collides with an element name is ambiguous.",
                col_decl.name, name);

    col_decl.setDefaultExpression(std::move(built_default));
    col_decl.default_specifier = ColumnDefaultSpecifier::Default;
}

}
