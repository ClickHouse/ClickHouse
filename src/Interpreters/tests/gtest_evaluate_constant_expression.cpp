#include <Core/Field.h>
#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <Core/ConstantValue.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <gtest/gtest.h>

using namespace DB;

/// The legacy `Field`-returning `evaluateConstantExpression` must preserve the exact `Field` type
/// tags of a literal node - in particular `Bool` elements of `array(...)` / `tuple(...)` (and a bare
/// scalar `Bool`) must stay `Bool`, not collapse to `UInt64`. Callers still convert this `Field`
/// (e.g. `TableFunctionValues`), so a collapsed tag changes results: `values('x String', true)` would
/// return `'1'` instead of `'true'`. Literal nodes therefore take a tag-preserving fast path instead
/// of round-tripping through the size-1 column - a temporary compatibility shim that is removed
/// together with the `Field`-returning API once its callers move to the column API.
TEST(EvaluateConstantExpression, LiteralPreservesNestedBoolTags)
{
    const auto & context = getContext().context;

    {
        const ASTPtr literal = make_intrusive<ASTLiteral>(Field(true));
        const Field field = evaluateConstantExpression(literal, context).first;
        EXPECT_EQ(field.getType(), Field::Types::Bool);
    }

    {
        const ASTPtr literal = make_intrusive<ASTLiteral>(Field(Array{Field(true), Field(false)}));
        const Field field = evaluateConstantExpression(literal, context).first;
        ASSERT_EQ(field.getType(), Field::Types::Array);
        const Array & array = field.safeGet<Array>();
        ASSERT_EQ(array.size(), 2u);
        EXPECT_EQ(array[0].getType(), Field::Types::Bool);
        EXPECT_EQ(array[1].getType(), Field::Types::Bool);
    }

    {
        const ASTPtr literal = make_intrusive<ASTLiteral>(Field(Tuple{Field(true), Field(false)}));
        const Field field = evaluateConstantExpression(literal, context).first;
        ASSERT_EQ(field.getType(), Field::Types::Tuple);
        const Tuple & tuple = field.safeGet<Tuple>();
        ASSERT_EQ(tuple.size(), 2u);
        EXPECT_EQ(tuple[0].getType(), Field::Types::Bool);
        EXPECT_EQ(tuple[1].getType(), Field::Types::Bool);
    }
}

/// The column API is type-faithful: it keeps the exact SQL type (`Array(Bool)`) with no `Field` tag
/// collapse. This is the replacement callers should migrate to as part of removing `Field`.
TEST(EvaluateConstantExpression, ColumnApiPreservesExactType)
{
    const auto & context = getContext().context;

    const ASTPtr array_literal = make_intrusive<ASTLiteral>(Field(Array{Field(true), Field(false)}));
    const auto value = evaluateConstantExpressionAsColumn(array_literal, context);
    EXPECT_EQ(value.getType()->getName(), "Array(Bool)");
    ASSERT_EQ(value.getColumn()->size(), 1u);
}

/// The `Field`-returning `evaluateConstantExpression` on a NON-literal constant goes through the
/// `materializeToField` compatibility bridge, which must read the value via the column's `operator[]`
/// (matching the historical non-literal path, including the `Bool`->`UInt64` canonicalization and the
/// unwrapping of a `Nullable`). The analyzer path is forced so a non-literal reaches that bridge rather
/// than being folded to a literal.
TEST(EvaluateConstantExpression, NonLiteralFieldBridge)
{
    tryRegisterFunctions();

    auto context = Context::createCopy(getContext().context);
    context->setSetting("allow_experimental_analyzer", Field(true));

    /// Plain arithmetic: value is preserved.
    {
        const ASTPtr expr = makeASTFunction("plus",
            make_intrusive<ASTLiteral>(Field(UInt64(2))), make_intrusive<ASTLiteral>(Field(UInt64(3))));
        const Field field = evaluateConstantExpression(expr, context).first;
        EXPECT_EQ(field, Field(UInt64(5)));
    }

    /// A `Bool` result reaches the bridge as a non-literal and canonicalizes to `UInt64` (historical).
    {
        const ASTPtr expr = makeASTFunction("equals",
            make_intrusive<ASTLiteral>(Field(UInt64(1))), make_intrusive<ASTLiteral>(Field(UInt64(1))));
        const Field field = evaluateConstantExpression(expr, context).first;
        EXPECT_EQ(field, Field(UInt64(1)));
        EXPECT_EQ(field.getType(), Field::Types::UInt64);
    }

    /// A `Nullable` wrapper: a non-NULL value materializes to the underlying value.
    {
        const ASTPtr expr = makeASTFunction("toNullable", make_intrusive<ASTLiteral>(Field(UInt64(7))));
        const Field field = evaluateConstantExpression(expr, context).first;
        EXPECT_EQ(field, Field(UInt64(7)));
    }
}
