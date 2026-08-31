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

/// Build `CAST(<expression>, '<type>')`. Used for a default pulled up out of a `Variant` alternative:
/// a value is convertible to a `Variant` only when its type is exactly one of the alternatives, so
/// the built tuple has to be brought to the type of the alternative it came from first.
ASTPtr makeCastToType(ASTPtr expression, const IAST & type)
{
    return makeASTFunction("CAST", std::move(expression), make_intrusive<ASTLiteral>(type.formatForLogging()));
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

/// A DEFAULT expression may reference other columns of the table, but not elements of the tuple it
/// is written in, nor elements of an enclosing tuple: after the pull-up such a reference would be
/// resolved against the table columns, so a name that collides with a visible element name is
/// ambiguous and is rejected. Only the element names visible at the place of the default are
/// considered - elements of unrelated (sibling or deeper) tuples cannot be referenced at all, so
/// reusing their names elsewhere in the table is not ambiguous.
void checkDefaultDoesNotReferenceElements(const IAST & expression, const NameSet & visible_element_names, const String & column_name)
{
    NameSet referenced;
    NameSet bound;
    collectReferencedIdentifiers(expression, referenced, bound);

    /// The default cannot reference the column it is written in, neither directly (`c`) nor through
    /// subcolumn syntax (`c.a`): after the pull-up it becomes a self-referential column-level
    /// default. `collectReferencedIdentifiers` inserts the root of a compound identifier as well, so
    /// a reference such as `c.a` is caught by the column name alone. This has to be rejected here:
    /// the cycle detection in `ColumnsDescription` compares whole identifiers, so it does not
    /// recognize `c.a` as a reference to the column `c`, and the failure would be deferred to the
    /// first insert that omits the column.
    if (referenced.contains(column_name))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "DEFAULT expression inside the data type of column '{}' references the column itself. "
            "A default expression cannot depend on the column it defines.",
            column_name);

    for (const auto & name : referenced)
        if (visible_element_names.contains(name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "DEFAULT expression inside the data type of column '{}' references '{}', which is a tuple/nested "
                "element name. Default expressions cannot reference other elements of the same tuple/nested, and a "
                "reference that collides with an element name is ambiguous.",
                column_name, name);
}

/// Strip the DEFAULT expression from a name-type pair (turning it back into a plain element).
void stripDefaultFromNameTypePair(ASTNameTypePair & pair)
{
    pair.default_expression = nullptr;
    pair.children.clear();
    if (pair.type)
        pair.children.push_back(pair.type);
}

/// A type that already admits NULL and must not be wrapped again: `Nullable(...)` or
/// `LowCardinality(Nullable(...))`.
bool typeIsAlreadyNullable(const ASTPtr & type)
{
    const auto * data_type = type->as<ASTDataType>();
    if (!data_type)
        return false;

    if (data_type->name == "Nullable")
        return true;

    if (data_type->name == "LowCardinality")
    {
        const auto arguments = data_type->getArguments();
        if (arguments && arguments->children.size() == 1)
            if (const auto * inner = arguments->children[0]->as<ASTDataType>())
                return inner->name == "Nullable";
    }

    return false;
}

/// Match the `DEFAULT NULL` normalization for ordinary column declarations: NULL defaults promote
/// a non-nullable type to Nullable. LowCardinality can only wrap Nullable, not the other way round.
ASTPtr makeNullableType(ASTPtr type)
{
    if (const auto * data_type = type->as<ASTDataType>(); data_type && data_type->name == "LowCardinality")
    {
        const auto arguments = data_type->getArguments();
        if (arguments && arguments->children.size() == 1)
            return makeASTDataType("LowCardinality", makeASTDataType("Nullable", arguments->children[0]));
    }

    return makeASTDataType("Nullable", std::move(type));
}

bool isLiteralNull(const ASTPtr & expression)
{
    const auto * literal = expression ? expression->as<ASTLiteral>() : nullptr;
    return literal && literal->value.isNull();
}

/// Recursively walk a data type AST, building a column-level default expression from any DEFAULT
/// expressions reachable through a chain of Tuples and stripping them from the type. Returns the
/// default-value expression if the type (recursively) contains DEFAULTs, otherwise nullptr.
///
/// A DEFAULT inside a non-Tuple wrapper (Array, Map, Nested, ...) cannot be represented as a static
/// column default, so it is rejected with NOT_IMPLEMENTED.
///
/// `outer_element_names` holds the element names visible at this point of the walk (the elements of
/// the enclosing tuples); every explicit default is checked against the names visible where it is
/// written, which is why the check happens here rather than in the caller.
ASTPtr buildAndStripTupleDefaults(IAST & type, const NameSet & outer_element_names, const String & column_name)
{
    /// Named/unnamed tuple parsed via the fast path (ASTTupleDataType). It never carries DEFAULTs
    /// directly (those force the generic parser path), but its element types might contain them.
    if (auto * tuple = type.as<ASTTupleDataType>())
    {
        NameSet element_names = outer_element_names;
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
            element_defaults[i] = buildAndStripTupleDefaults(*arguments->children[i], element_names, column_name);
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

        /// Collect the names of all elements of this tuple first: a default written on the first
        /// element is equally ambiguous with respect to the name of the last one.
        NameSet element_names = outer_element_names;
        for (const auto & child : arguments->children)
            if (const auto * pair = child->as<ASTNameTypePair>(); pair && !pair->name.empty())
                element_names.insert(pair->name);

        bool has_default = false;
        ASTs element_types(arguments->children.size());
        ASTs element_defaults(arguments->children.size());
        for (size_t i = 0; i < arguments->children.size(); ++i)
        {
            ASTPtr & child = arguments->children[i];
            if (auto * pair = child->as<ASTNameTypePair>())
            {
                ASTPtr explicit_default = pair->default_expression;
                ASTPtr nested_default = buildAndStripTupleDefaults(*pair->type, element_names, column_name);

                if (explicit_default && nested_default)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Tuple element '{}' has both a DEFAULT expression and DEFAULT expressions inside its type",
                        pair->name);

                if (explicit_default)
                {
                    checkDefaultDoesNotReferenceElements(*explicit_default, element_names, column_name);
                    if (isLiteralNull(explicit_default) && !typeIsAlreadyNullable(pair->type))
                        pair->type = makeNullableType(pair->type);
                    stripDefaultFromNameTypePair(*pair);
                }

                element_types[i] = pair->type;
                element_defaults[i] = explicit_default ? explicit_default : nested_default;
            }
            else
            {
                element_types[i] = child;
                element_defaults[i] = buildAndStripTupleDefaults(*child, element_names, column_name);
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

                /// A default anywhere inside is rejected below, so the walk starts from an empty
                /// scope: `NOT_IMPLEMENTED` is the accurate diagnostic here, not an ambiguity error.
                if (pair->default_expression || buildAndStripTupleDefaults(*pair->type, {}, column_name))
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
        return buildAndStripTupleDefaults(*arguments->children[0], outer_element_names, column_name);
    }

    /// Variant is a transparent wrapper as well: a value of one of its alternatives is a valid value
    /// of the whole Variant, so a DEFAULT inside `Variant(..., Tuple(...))` is representable as the
    /// column-level default of that alternative. Conversion to a Variant requires the value to have
    /// exactly the type of an alternative, so the pulled-up tuple is cast to the alternative type
    /// before it becomes the column default.
    if (data_type->name == "Variant")
    {
        if (!arguments)
            return nullptr;

        ASTPtr result;
        for (const auto & child : arguments->children)
        {
            ASTPtr alternative_default = buildAndStripTupleDefaults(*child, outer_element_names, column_name);
            if (!alternative_default)
                continue;

            if (result)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "DEFAULT expressions inside the data type of column '{}' are written in more than one alternative "
                    "of a Variant. A column has a single default value, so at most one alternative may define it.",
                    column_name);

            result = makeCastToType(std::move(alternative_default), *child);
        }
        return result;
    }

    /// SimpleAggregateFunction is a transparent wrapper too: the column stores plain values of its
    /// storage type, which is the first type argument (`DataTypeCustomSimpleAggregateFunction::create`
    /// takes `argument_types[0]` as the storage type), and a plain value casts into the
    /// SimpleAggregateFunction type. So a DEFAULT inside `SimpleAggregateFunction(f, Tuple(...))` is
    /// representable as the column-level default built from the storage type. Further type arguments
    /// (if any) do not describe the stored value, so a DEFAULT inside them is not representable.
    if (data_type->name == "SimpleAggregateFunction")
    {
        if (!arguments || arguments->children.size() < 2)
            return nullptr;

        /// children[0] is the aggregate function, children[1] is the storage type.
        ASTPtr result = buildAndStripTupleDefaults(*arguments->children[1], outer_element_names, column_name);

        for (size_t i = 2; i < arguments->children.size(); ++i)
        {
            /// As for `Nested`: an empty scope, so that an unsupported default is reported as such.
            if (buildAndStripTupleDefaults(*arguments->children[i], {}, column_name))
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "DEFAULT expressions are not supported inside an argument type of SimpleAggregateFunction other "
                    "than the first one (the storage type)");
        }
        return result;
    }

    /// Any other composite type (Array, Map, LowCardinality, ...): a DEFAULT inside is not
    /// representable as a static column default.
    if (arguments)
    {
        for (const auto & child : arguments->children)
        {
            /// As for `Nested`: an empty scope, so that an unsupported default is reported as such.
            if (buildAndStripTupleDefaults(*child, {}, column_name))
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

    /// The ambiguity check for the individual defaults happens during the walk, where the set of
    /// element names visible at each default is known.
    ASTPtr built_default = buildAndStripTupleDefaults(*type, {}, col_decl.name);
    if (!built_default)
        return;

    if (col_decl.default_specifier != ColumnDefaultSpecifier::Empty || col_decl.getDefaultExpression())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Column '{}' cannot have both a column-level default and DEFAULT expressions inside its data type",
            col_decl.name);

    col_decl.setDefaultExpression(std::move(built_default));
    col_decl.default_specifier = ColumnDefaultSpecifier::Default;
}

}
