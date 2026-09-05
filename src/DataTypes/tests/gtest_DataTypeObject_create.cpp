#include <Common/assert_cast.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/IDataType.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTObjectTypeArgument.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

void expectSubcolumnType(const IDataType & type, std::string_view subcolumn, std::string_view expected_name)
{
    SCOPED_TRACE(String(subcolumn));
    auto result = type.tryGetSubcolumnType(subcolumn);
    EXPECT_EQ(type.hasSubcolumn(subcolumn), !expected_name.empty());

    if (expected_name.empty())
    {
        EXPECT_EQ(result, nullptr);
        EXPECT_THROW(type.getSubcolumnType(subcolumn), Exception);
        return;
    }

    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->getName(), expected_name);
    EXPECT_TRUE(result->equals(*type.getSubcolumnType(subcolumn)));
}

}

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

TEST(DataTypeObject, TypeOnlySubcolumnLookup)
{
    auto type = DataTypeFactory::instance().get(
        "JSON("
        "max_dynamic_types=5, max_dynamic_paths=3, "
        "typed_string String, typed_array Array(UInt64), "
        "nested Array(JSON(inner UInt32)), nullable Nullable(String), "
        "a Array(JSON), a.b Int64, `escaped.dot` UInt8, `spaced path` UInt8, obj.x UInt16)");
    const auto & object = assert_cast<const DataTypeObject &>(*type);

    const std::vector<std::pair<String, String>> subcolumns = {
        {"typed_string", "String"},
        {"typed_array", "Array(UInt64)"},
        {"typed_array.size0", "UInt64"},
        {"arbitrary", "Dynamic(max_types=5)"},
        {"arbitrary.:`Int64`", "Nullable(Int64)"},
        {"arbitrary.:`Array(String)`.size0", "UInt64"},
        {"nested.inner", "Array(UInt32)"},
        {"nested.inner.:`UInt32`", "Array(UInt32)"},
        {"a.x", "Array(Dynamic)"},
        {"a.b", "Int64"},
        {"a.b.c", "Array(Dynamic)"},
        {"a.:`Array(JSON)`.x", "Array(Dynamic)"},
        {"a.:`String`", "Array(Dynamic)"},
        {"nullable", "Nullable(String)"},
        {"escaped.dot", "UInt8"},
        {"spaced path", "UInt8"},
        {"^`obj`", "JSON(max_dynamic_types=5, max_dynamic_paths=3, x UInt16)"},
        {"@`obj`.x", "UInt16"},
        {"@`missing`", "Dynamic(max_types=5)"},
        {DataTypeObject::SPECIAL_SUBCOLUMN_NAME_FOR_DISTINCT_PATHS_CALCULATION, "Array(String)"},
        {":`Int64`", ""},
    };

    for (const auto & [subcolumn, expected_name] : subcolumns)
        expectSubcolumnType(object, subcolumn, expected_name);
}

TEST(DataTypeObject, TypeOnlySubcolumnLookupPreservesSerializationPrecedence)
{
    auto & factory = DataTypeFactory::instance();

    auto array_collision = factory.get("JSON(a Array(UInt64), a.size0 String)");
    expectSubcolumnType(*array_collision, "a.size0", "UInt64");

    auto tuple_collision = factory.get("JSON(a Tuple(size0 String), a.size0 UInt64)");
    expectSubcolumnType(*tuple_collision, "a.size0", "String");

    auto multiple_prefixes = factory.get("JSON(a Array(JSON), a.b Int64)");
    expectSubcolumnType(*multiple_prefixes, "a.b", "Int64");
    expectSubcolumnType(*multiple_prefixes, "a.b.c", "Array(Dynamic)");

    auto special_name_collision = factory.get(fmt::format(
        "JSON({} UInt8)", DataTypeObject::SPECIAL_SUBCOLUMN_NAME_FOR_DISTINCT_PATHS_CALCULATION));
    expectSubcolumnType(
        *special_name_collision, DataTypeObject::SPECIAL_SUBCOLUMN_NAME_FOR_DISTINCT_PATHS_CALCULATION, "UInt8");

    auto prefixed_name_collision = std::make_shared<DataTypeObject>(
        DataTypeObject::SchemaFormat::JSON,
        std::unordered_map<String, DataTypePtr>{{"^`obj`", factory.get("JSON(x UInt8)")}});
    expectSubcolumnType(*prefixed_name_collision, "^`obj`.x", "UInt8");
}

TEST(DataTypeObject, TypeOnlySubcolumnLookupUsesCanonicalPathNames)
{
    auto nested_type = DataTypeFactory::instance().get("JSON(x UInt8)");
    auto object = std::make_shared<DataTypeObject>(
        DataTypeObject::SchemaFormat::JSON,
        std::unordered_map<String, DataTypePtr>{
            {"escaped.dot", nested_type},
            {R"(back\slash)", nested_type},
            {"back`tick", nested_type},
            {"spaced path", nested_type},
        });

    expectSubcolumnType(*object, "escaped.dot.x", "UInt8");
    expectSubcolumnType(*object, R"(back\slash.x)", "UInt8");
    expectSubcolumnType(*object, "back`tick.x", "UInt8");
    expectSubcolumnType(*object, "spaced path.x", "UInt8");
}

TEST(DataTypeObject, TypeOnlySubcolumnLookupDefersToCustomSerialization)
{
    auto object = std::make_shared<DataTypeObject>(
        DataTypeObject::SchemaFormat::JSON,
        std::unordered_map<String, DataTypePtr>{{"a", DataTypeFactory::instance().get("UInt8")}});
    auto serialization = object->getDefaultSerialization();
    object->setCustomization(std::make_unique<DataTypeCustomDesc>(DataTypeCustomNamePtr{}, std::move(serialization)));

    expectSubcolumnType(*object, "a", "UInt8");
}
