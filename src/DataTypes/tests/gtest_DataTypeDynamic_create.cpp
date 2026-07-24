#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>

#include <gtest/gtest.h>

using namespace DB;

/// Regression test for BuzzHouse segfault: null pointer dereference when
/// DataTypeDynamic::create() receives a malformed AST where the LHS of
/// the equals operator is not an ASTIdentifier (e.g., a literal).
/// See: https://github.com/ClickHouse/ClickHouse/issues/101309
/// STID: 1320-2f88
TEST(DataTypeDynamic, CreateWithMalformedASTDoesNotCrash)
{
    auto & factory = DataTypeFactory::instance();

    /// Construct a malformed AST: Dynamic(42 = 5)
    /// The "42" is an ASTLiteral, not an ASTIdentifier.
    /// This triggers the error path in DataTypeDynamic::create() at the
    /// `if (!identifier)` check. Before the fix, the error message
    /// dereferenced the null identifier pointer -> segfault.
    auto equals_args = make_intrusive<ASTExpressionList>();
    equals_args->children.push_back(make_intrusive<ASTLiteral>(Field(UInt64(42))));
    equals_args->children.push_back(make_intrusive<ASTLiteral>(Field(UInt64(5))));

    auto equals_func = make_intrusive<ASTFunction>();
    equals_func->name = "equals";
    equals_func->arguments = equals_args;
    equals_func->children.push_back(equals_args);

    auto arguments = make_intrusive<ASTExpressionList>();
    arguments->children.push_back(equals_func);

    /// Must throw UNEXPECTED_AST_STRUCTURE, not crash with SIGSEGV.
    EXPECT_THROW(factory.get("Dynamic", arguments), Exception);
}

/// Verify that valid Dynamic(max_types=N) still works correctly.
TEST(DataTypeDynamic, CreateWithValidAST)
{
    auto & factory = DataTypeFactory::instance();

    auto type = factory.get("Dynamic(max_types=5)");
    ASSERT_NE(type, nullptr);
    EXPECT_EQ(type->getName(), "Dynamic(max_types=5)");

    auto type_default = factory.get("Dynamic");
    ASSERT_NE(type_default, nullptr);
    EXPECT_EQ(type_default->getName(), "Dynamic");
}
