#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTObjectTypeArgument.h>

#include <gtest/gtest.h>

using namespace DB;

/// Regression tests for STID 1477-2777 (BuzzHouse serverfuzz, amd_msan, 2026-04-25):
/// `DataTypeObject::createObject` accessed `object_type_argument->parameter`,
/// `path_with_type`, and `function->arguments->children[0]` / `[1]` without
/// validating the AST shape. The server-side AST fuzzer's
/// `fuzzColumnLikeExpressionList` and `fuzzExpressionList` mutators can drop
/// children or replace them with mismatched node types, producing ASTs that
/// the type factory then segfaulted on. These tests construct the same
/// malformed shapes directly so the regression is exercised deterministically
/// without any fuzzer.

/// A child of the `JSON(...)` argument list is not an `ASTObjectTypeArgument`
/// (e.g. an `ASTLiteral` substituted by the fuzzer). Before the fix,
/// `object_type_argument->parameter` dereferenced a null pointer.
TEST(DataTypeObject, CreateJSONWithNonObjectTypeArgumentChildDoesNotCrash)
{
    auto & factory = DataTypeFactory::instance();

    auto arguments = make_intrusive<ASTExpressionList>();
    arguments->children.push_back(make_intrusive<ASTLiteral>(Field(UInt64(42))));

    EXPECT_THROW(factory.get("JSON", arguments), Exception);
}

/// The `parameter` of an `ASTObjectTypeArgument` is an `ASTFunction` named
/// `equals` whose `arguments` is null. Before the fix, the code accessed
/// `function->arguments->children[0]` and segfaulted in
/// `boost::intrusive_ptr::operator bool`.
TEST(DataTypeObject, CreateJSONWithEqualsNullArgumentsDoesNotCrash)
{
    auto & factory = DataTypeFactory::instance();

    auto equals_func = make_intrusive<ASTFunction>();
    equals_func->name = "equals";
    /// `arguments` deliberately left null.

    auto object_arg = make_intrusive<ASTObjectTypeArgument>();
    object_arg->parameter = equals_func;
    object_arg->children.push_back(equals_func);

    auto arguments = make_intrusive<ASTExpressionList>();
    arguments->children.push_back(object_arg);

    EXPECT_THROW(factory.get("JSON", arguments), Exception);
}

/// The `equals` function has `arguments` with zero children — `children[0]`
/// reads past the end of the vector.
TEST(DataTypeObject, CreateJSONWithEqualsZeroChildrenDoesNotCrash)
{
    auto & factory = DataTypeFactory::instance();

    auto equals_args = make_intrusive<ASTExpressionList>();

    auto equals_func = make_intrusive<ASTFunction>();
    equals_func->name = "equals";
    equals_func->arguments = equals_args;
    equals_func->children.push_back(equals_args);

    auto object_arg = make_intrusive<ASTObjectTypeArgument>();
    object_arg->parameter = equals_func;
    object_arg->children.push_back(equals_func);

    auto arguments = make_intrusive<ASTExpressionList>();
    arguments->children.push_back(object_arg);

    EXPECT_THROW(factory.get("JSON", arguments), Exception);
}

/// The `equals` function has only one child — `children[1]` reads past the end.
TEST(DataTypeObject, CreateJSONWithEqualsOneChildDoesNotCrash)
{
    auto & factory = DataTypeFactory::instance();

    auto equals_args = make_intrusive<ASTExpressionList>();
    equals_args->children.push_back(make_intrusive<ASTIdentifier>("max_dynamic_paths"));

    auto equals_func = make_intrusive<ASTFunction>();
    equals_func->name = "equals";
    equals_func->arguments = equals_args;
    equals_func->children.push_back(equals_args);

    auto object_arg = make_intrusive<ASTObjectTypeArgument>();
    object_arg->parameter = equals_func;
    object_arg->children.push_back(equals_func);

    auto arguments = make_intrusive<ASTExpressionList>();
    arguments->children.push_back(object_arg);

    EXPECT_THROW(factory.get("JSON", arguments), Exception);
}

/// The `parameter` of an `ASTObjectTypeArgument` is a function that is NOT
/// `equals`. Before the fix, the error message called `formatForErrorMessage`
/// on the null `function` pointer after a failed `as<ASTFunction>` cast on the
/// expected branch — but the cast itself succeeds here, so we exercise the
/// "wrong function name" path.
TEST(DataTypeObject, CreateJSONWithParameterWrongFunctionNameIsRejected)
{
    auto & factory = DataTypeFactory::instance();

    auto wrong_func = make_intrusive<ASTFunction>();
    wrong_func->name = "plus";

    auto object_arg = make_intrusive<ASTObjectTypeArgument>();
    object_arg->parameter = wrong_func;
    object_arg->children.push_back(wrong_func);

    auto arguments = make_intrusive<ASTExpressionList>();
    arguments->children.push_back(object_arg);

    EXPECT_THROW(factory.get("JSON", arguments), Exception);
}

/// The `parameter` of an `ASTObjectTypeArgument` is not an `ASTFunction` at all
/// (e.g. an `ASTLiteral` substituted in by the fuzzer). Before the fix, the
/// error message dereferenced the null `function` pointer returned by the
/// failed `as<ASTFunction>` cast -> segfault.
TEST(DataTypeObject, CreateJSONWithParameterNotAFunctionDoesNotCrash)
{
    auto & factory = DataTypeFactory::instance();

    auto literal = make_intrusive<ASTLiteral>(Field(UInt64(42)));

    auto object_arg = make_intrusive<ASTObjectTypeArgument>();
    object_arg->parameter = literal;
    object_arg->children.push_back(literal);

    auto arguments = make_intrusive<ASTExpressionList>();
    arguments->children.push_back(object_arg);

    EXPECT_THROW(factory.get("JSON", arguments), Exception);
}

/// The `path_with_type` of an `ASTObjectTypeArgument` is not an
/// `ASTObjectTypedPathArgument` (e.g. an `ASTLiteral` substituted in by the
/// fuzzer). Before the fix, the code accessed `path_with_type->type` and
/// `->path` on the null pointer returned by the failed cast -> segfault.
TEST(DataTypeObject, CreateJSONWithPathWithTypeWrongKindDoesNotCrash)
{
    auto & factory = DataTypeFactory::instance();

    auto literal = make_intrusive<ASTLiteral>(Field(UInt64(42)));

    auto object_arg = make_intrusive<ASTObjectTypeArgument>();
    object_arg->path_with_type = literal;
    object_arg->children.push_back(literal);

    auto arguments = make_intrusive<ASTExpressionList>();
    arguments->children.push_back(object_arg);

    EXPECT_THROW(factory.get("JSON", arguments), Exception);
}

/// The `skip_path_regexp` of an `ASTObjectTypeArgument` is not a string literal
/// (e.g. an identifier substituted in by the fuzzer). Before the fix, the
/// error message called `formatForErrorMessage` on `object_type_argument->skip_path`,
/// which is null in this branch -> segfault.
TEST(DataTypeObject, CreateJSONWithSkipPathRegexpWrongKindDoesNotCrash)
{
    auto & factory = DataTypeFactory::instance();

    /// Not a string literal — an identifier.
    auto wrong_node = make_intrusive<ASTIdentifier>("not_a_regexp");

    auto object_arg = make_intrusive<ASTObjectTypeArgument>();
    object_arg->skip_path_regexp = wrong_node;
    object_arg->children.push_back(wrong_node);

    auto arguments = make_intrusive<ASTExpressionList>();
    arguments->children.push_back(object_arg);

    EXPECT_THROW(factory.get("JSON", arguments), Exception);
}

/// Verify well-formed `JSON(...)` types still parse correctly.
TEST(DataTypeObject, CreateJSONWithValidAST)
{
    auto & factory = DataTypeFactory::instance();

    auto type = factory.get("JSON(max_dynamic_paths=100)");
    ASSERT_NE(type, nullptr);

    auto type_default = factory.get("JSON");
    ASSERT_NE(type_default, nullptr);
}
