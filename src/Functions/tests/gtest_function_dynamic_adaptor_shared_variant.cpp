#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Field.h>

using namespace DB;

/// A Dynamic column can hold the same logical type both as a regular variant and inside the shared
/// variant at the same time. Applying a function over such a column must keep separate selector slots
/// for the two storages, otherwise the per-variant column sizes stop matching the scattered arguments.
TEST(FunctionDynamicAdaptor, SameTypeInSharedAndRegularVariant)
{
    tryRegisterFunctions();
    auto context = getContext().context;

    auto dyn_type = std::make_shared<DataTypeDynamic>(2);
    auto column = ColumnDynamic::create(2);

    /// Put several Int8 values into the shared variant.
    auto int8_type = std::make_shared<DataTypeInt8>();
    auto shared_src = int8_type->createColumn();
    shared_src->insert(Field(Int8(20)));
    shared_src->insert(Field(Int8(21)));
    shared_src->insert(Field(Int8(22)));
    column->insertValueIntoSharedVariant(*shared_src, int8_type, int8_type->getName(), 0);
    column->insertValueIntoSharedVariant(*shared_src, int8_type, int8_type->getName(), 1);
    column->insertValueIntoSharedVariant(*shared_src, int8_type, int8_type->getName(), 2);

    /// Add regular Int8 values: same type, non-shared variant.
    column->insert(Field(Int8(10)));
    column->insert(Field(Int8(11)));

    const size_t n = column->size();
    ASSERT_EQ(n, 5u);
    ASSERT_EQ(column->getSharedVariant().size(), 3u);

    /// A binary function with a non-const second argument routes through FunctionDynamicAdaptor and
    /// scatters the second argument per variant.
    auto rhs_type = std::make_shared<DataTypeInt64>();
    auto rhs = rhs_type->createColumn();
    for (size_t i = 0; i < n; ++i)
        rhs->insert(Field(Int64(100)));

    auto resolver = FunctionFactory::instance().get("plus", context);
    ColumnsWithTypeAndName args;
    args.emplace_back(ColumnWithTypeAndName{std::move(column), dyn_type, "d"});
    args.emplace_back(ColumnWithTypeAndName{std::move(rhs), rhs_type, "rhs"});
    auto func = resolver->build(args);

    ColumnPtr result;
    ASSERT_NO_THROW(result = func->execute(args, func->getResultType(), n, false));
    ASSERT_EQ(result->size(), n);
}
