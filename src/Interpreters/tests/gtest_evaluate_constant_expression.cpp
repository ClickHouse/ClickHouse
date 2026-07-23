#include <Core/Field.h>
#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>
#include <Common/tests/gtest_global_context.h>

#include <gtest/gtest.h>

using namespace DB;

/// The legacy `Field`-returning `evaluateConstantExpression` is lossy: it produces the value as a
/// size-1 column and reads it back with `operator[]`, which re-canonicalizes nested `Field` type
/// tags through the column implementation. So `Bool` elements of a literal `array(...)` / `tuple(...)`
/// come back as `UInt64` (the nested column of `Array(Bool)` is a plain `ColumnUInt8`). This is one
/// of the reasons `Field` is being removed; it is documented here so the behavior is not changed
/// unknowingly while callers are migrated off the `Field` API. Note that it is not user-reachable:
/// strict `Bool` consumers use `evaluateConstantExpressionAsLiteral` (which returns literal nodes
/// verbatim) or `Array(Variant)` (which is per-element type-aware), and tolerant consumers pass the
/// value through `convertFieldToType`.
TEST(EvaluateConstantExpression, FieldApiCanonicalizesNestedBoolTags)
{
    const auto & context = getContext().context;

    const ASTPtr array_literal = make_intrusive<ASTLiteral>(Field(Array{Field(true), Field(false)}));
    const Field array_field = evaluateConstantExpression(array_literal, context).first;
    ASSERT_EQ(array_field.getType(), Field::Types::Array);
    EXPECT_EQ(array_field.safeGet<Array>().at(0).getType(), Field::Types::UInt64);

    const ASTPtr tuple_literal = make_intrusive<ASTLiteral>(Field(Tuple{Field(true), Field(false)}));
    const Field tuple_field = evaluateConstantExpression(tuple_literal, context).first;
    ASSERT_EQ(tuple_field.getType(), Field::Types::Tuple);
    EXPECT_EQ(tuple_field.safeGet<Tuple>().at(0).getType(), Field::Types::UInt64);
}

/// The column API is type-faithful: it keeps the exact SQL type (`Array(Bool)`) with no `Field` tag
/// collapse. This is the replacement callers should migrate to as part of removing `Field`.
TEST(EvaluateConstantExpression, ColumnApiPreservesExactType)
{
    const auto & context = getContext().context;

    const ASTPtr array_literal = make_intrusive<ASTLiteral>(Field(Array{Field(true), Field(false)}));
    const auto [column, type] = evaluateConstantExpressionAsColumn(array_literal, context);
    EXPECT_EQ(type->getName(), "Array(Bool)");
    ASSERT_EQ(column->size(), 1u);
}
