#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTObjectTypeArgument.h>

#include <gtest/gtest.h>

using namespace DB;

/// Regression test for BuzzHouse segfault: null pointer dereference when
/// `DataTypeDynamic::create` receives a malformed AST where the LHS of
/// the `equals` operator is not an `ASTIdentifier` (e.g., a literal).
/// See: https://github.com/ClickHouse/ClickHouse/issues/101309
/// STID: 1320-2f88
TEST(DataTypeDynamic, CreateWithMalformedASTDoesNotCrash)
{
    auto & factory = DataTypeFactory::instance();

    /// Construct a malformed AST: `Dynamic(42 = 5)`.
    /// The `42` is an `ASTLiteral`, not an `ASTIdentifier`.
    /// This triggers the error path in `DataTypeDynamic::create` at the
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

    /// Must throw `UNEXPECTED_AST_STRUCTURE`, not crash with SIGSEGV.
    EXPECT_THROW(factory.get("Dynamic", arguments), Exception);
}

/// Regression test for STID 1477-2777 (BuzzHouse serverfuzz, amd_msan, 2026-04-25):
/// the `equals` function inside a `Dynamic` argument has `arguments == nullptr`.
/// Before the fix, `DataTypeDynamic::create` accessed
/// `argument->arguments->children[0]` without validating `arguments`, dereferencing
/// a null `boost::intrusive_ptr` -> segfault.
TEST(DataTypeDynamic, CreateWithEqualsNullArgumentsDoesNotCrash)
{
    auto & factory = DataTypeFactory::instance();

    auto equals_func = make_intrusive<ASTFunction>();
    equals_func->name = "equals";
    /// `arguments` deliberately left null.

    auto arguments = make_intrusive<ASTExpressionList>();
    arguments->children.push_back(equals_func);

    EXPECT_THROW(factory.get("Dynamic", arguments), Exception);
}

/// The `equals` function has `arguments` with zero children — `children[0]` would
/// read past the end of the vector. Before the fix, this segfaulted inside
/// `boost::container::vector::operator[]`.
TEST(DataTypeDynamic, CreateWithEqualsZeroChildrenDoesNotCrash)
{
    auto & factory = DataTypeFactory::instance();

    auto equals_args = make_intrusive<ASTExpressionList>();
    /// No children pushed.

    auto equals_func = make_intrusive<ASTFunction>();
    equals_func->name = "equals";
    equals_func->arguments = equals_args;
    equals_func->children.push_back(equals_args);

    auto arguments = make_intrusive<ASTExpressionList>();
    arguments->children.push_back(equals_func);

    EXPECT_THROW(factory.get("Dynamic", arguments), Exception);
}

/// The `equals` function has only one child — `children[1]` reads past the end.
TEST(DataTypeDynamic, CreateWithEqualsOneChildDoesNotCrash)
{
    auto & factory = DataTypeFactory::instance();

    auto equals_args = make_intrusive<ASTExpressionList>();
    equals_args->children.push_back(make_intrusive<ASTIdentifier>("max_types"));

    auto equals_func = make_intrusive<ASTFunction>();
    equals_func->name = "equals";
    equals_func->arguments = equals_args;
    equals_func->children.push_back(equals_args);

    auto arguments = make_intrusive<ASTExpressionList>();
    arguments->children.push_back(equals_func);

    EXPECT_THROW(factory.get("Dynamic", arguments), Exception);
}

/// The `equals` function has three children — accepted with the old indexing-only
/// shape check, but our validation rejects it as malformed.
TEST(DataTypeDynamic, CreateWithEqualsThreeChildrenIsRejected)
{
    auto & factory = DataTypeFactory::instance();

    auto equals_args = make_intrusive<ASTExpressionList>();
    equals_args->children.push_back(make_intrusive<ASTIdentifier>("max_types"));
    equals_args->children.push_back(make_intrusive<ASTLiteral>(Field(UInt64(5))));
    equals_args->children.push_back(make_intrusive<ASTLiteral>(Field(UInt64(99))));

    auto equals_func = make_intrusive<ASTFunction>();
    equals_func->name = "equals";
    equals_func->arguments = equals_args;
    equals_func->children.push_back(equals_args);

    auto arguments = make_intrusive<ASTExpressionList>();
    arguments->children.push_back(equals_func);

    EXPECT_THROW(factory.get("Dynamic", arguments), Exception);
}

/// Verify that valid `Dynamic(max_types=N)` still works correctly.
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
