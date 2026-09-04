#include <Columns/ColumnArray.h>
#include <Common/SettingsChanges.h>
#include <Common/assert_cast.h>
#include <Common/tests/gtest_global_context.h>
#include <Core/ProtocolDefines.h>
#include <Core/TypedQueryParameters.h>
#include <DataTypes/DataTypeFactory.h>
#include <Formats/FormatSettings.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/ReplaceQueryParameterVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTQueryParameter.h>
#include <Parsers/FieldFromAST.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

TypedQueryParameter makeParameter(const String & type_name, const String & value)
{
    auto type = DataTypeFactory::instance().get(type_name);
    auto column = type->createColumn();
    ReadBufferFromString input(value);
    type->getDefaultSerialization()->deserializeTextEscaped(*column, input, FormatSettings{});
    return {
        .type = std::move(type),
        .column = std::move(column),
        .value_hash = {},
        .scalar_name = {},
    };
}

ContextMutablePtr makeQueryContext()
{
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();
    return context;
}

}

TEST(TypedQueryParameters, NativeBlockRoundTripPreservesArrayColumn)
{
    const auto parameter = makeParameter("Array(Float32)", "[1.25,-2.5,3.75]");
    Block source({{parameter.column, parameter.type, "vector"}});

    WriteBufferFromOwnString output;
    NativeWriter writer(output, DBMS_TCP_PROTOCOL_VERSION, std::make_shared<const Block>(source.cloneEmpty()));
    writer.write(source);

    ReadBufferFromString input(output.str());
    NativeReader reader(input, DBMS_TCP_PROTOCOL_VERSION);
    Block decoded = reader.read();

    ASSERT_EQ(decoded.rows(), 1);
    ASSERT_EQ(decoded.columns(), 1);
    EXPECT_EQ(decoded.getByName("vector").type->getName(), "Array(Float32)");

    const auto & array = assert_cast<const ColumnArray &>(*decoded.getByName("vector").column);
    EXPECT_EQ(array.getData().size(), 3);
    EXPECT_EQ(array.getData()[0].safeGet<Float64>(), 1.25);
    EXPECT_EQ(array.getData()[1].safeGet<Float64>(), -2.5);
    EXPECT_EQ(array.getData()[2].safeGet<Float64>(), 3.75);
}

TEST(TypedQueryParameters, VisitorUsesColumnBackedScalar)
{
    auto context = makeQueryContext();
    context->addTypedQueryParameters({{"vector", makeParameter("Array(Float32)", "[1,2,3]")}});

    ASTPtr ast = make_intrusive<ASTQueryParameter>("vector", "Array(Float32)");
    ReplaceQueryParameterVisitor visitor(context);
    visitor.visit(ast);

    ASSERT_EQ(visitor.getNumberOfReplacedTypedParameters(), 1);
    const auto & function = ast->as<ASTFunction &>();
    EXPECT_EQ(function.name, "__getScalar");

    const auto & parameter = context->getTypedQueryParameters().at("vector");
    ASSERT_TRUE(context->hasScalar(parameter.scalar_name));
    const auto scalar = context->getScalar(parameter.scalar_name).getByPosition(0);
    EXPECT_EQ(scalar.column.get(), parameter.column.get());
    EXPECT_EQ(scalar.type->getName(), "Array(Float32)");
}

TEST(TypedQueryParameters, RejectsTypeMismatchAndTextCollision)
{
    auto context = makeQueryContext();
    context->addTypedQueryParameters({{"vector", makeParameter("Array(Float32)", "[1,2,3]")}});

    ASTPtr ast = make_intrusive<ASTQueryParameter>("vector", "Array(Float64)");
    ReplaceQueryParameterVisitor visitor(context);
    EXPECT_THROW(visitor.visit(ast), Exception);

    auto collision_context = makeQueryContext();
    collision_context->addQueryParameters({{"vector", "[1,2,3]"}});
    EXPECT_THROW(
        collision_context->addTypedQueryParameters({{"vector", makeParameter("Array(Float32)", "[1,2,3]")}}),
        Exception);

    auto reverse_collision_context = makeQueryContext();
    reverse_collision_context->addTypedQueryParameters({{"vector", makeParameter("Array(Float32)", "[1,2,3]")}});
    EXPECT_THROW(reverse_collision_context->addQueryParameters({{"vector", "[1,2,3]"}}), Exception);
    EXPECT_THROW(reverse_collision_context->setQueryParameter("vector", "[1,2,3]"), Exception);
}

TEST(TypedQueryParameters, ValueHashDependsOnTypeAndValue)
{
    const auto first = makeParameter("Array(Float32)", "[1,2,3]");
    const auto second = makeParameter("Array(Float32)", "[1,2,4]");
    const auto third = makeParameter("Array(Float64)", "[1,2,3]");

    EXPECT_NE(
        calculateTypedQueryParameterHash(*first.type, *first.column),
        calculateTypedQueryParameterHash(*second.type, *second.column));
    EXPECT_NE(
        calculateTypedQueryParameterHash(*first.type, *first.column),
        calculateTypedQueryParameterHash(*third.type, *third.column));
}

TEST(TypedQueryParameters, RejectsUnsupportedTypeAndInvalidRowCount)
{
    auto dynamic_type = DataTypeFactory::instance().get("Dynamic");
    EXPECT_THROW(validateTypedQueryParameterType(*dynamic_type), Exception);

    auto invalid = makeParameter("UInt64", "1");
    auto mutable_column = invalid.column->cloneResized(2);
    invalid.column = std::move(mutable_column);
    EXPECT_THROW(validateTypedQueryParameters({{"value", invalid}}), Exception);
}

TEST(TypedQueryParameters, SupportsNullableAndProtectsReservedScalar)
{
    auto context = makeQueryContext();
    context->addTypedQueryParameters({{"value", makeParameter("Nullable(UInt64)", "\\N")}});

    ASTPtr ast = make_intrusive<ASTQueryParameter>("value", "Nullable(UInt64)");
    ReplaceQueryParameterVisitor visitor(context);
    visitor.visit(ast);

    const auto & parameter = context->getTypedQueryParameters().at("value");
    EXPECT_TRUE(parameter.column->isNullAt(0));
    EXPECT_THROW(
        context->addScalar(parameter.scalar_name, Block({{parameter.column, parameter.type, "replacement"}})),
        Exception);
}

TEST(TypedQueryParameters, ResolvesTypedSettingValueWithoutTextParsing)
{
    auto context = makeQueryContext();
    context->addTypedQueryParameters({{"threads", makeParameter("UInt64", "7")}});

    ASTPtr ast = make_intrusive<ASTQueryParameter>("threads", "UInt64");
    SettingsChanges changes{{"max_threads", Field(CustomType(std::make_unique<FieldFromASTImpl>(ast)))}};
    ReplaceQueryParameterVisitor visitor(context);
    visitor.visitSettingsChanges(changes);

    ASSERT_EQ(visitor.getNumberOfReplacedTypedParameters(), 1);
    ASSERT_EQ(changes.size(), 1);
    EXPECT_EQ(changes.front().value.safeGet<UInt64>(), 7);
}
