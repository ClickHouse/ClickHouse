#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <Core/ColumnNumbers.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnDynamic.h>
#include <Interpreters/Context.h>

#include <Functions/isNull.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
}

FunctionPtr FunctionIsNull::create(ContextPtr context)
{
    return std::make_shared<FunctionIsNull>(context->getSettingsRef()[Setting::allow_experimental_analyzer]);
}

ColumnPtr FunctionIsNull::getConstantResultForNonConstArguments(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const
{
    /// (column IS NULL) triggers a bug in old analyzer when it is replaced to constant.
    if (!use_analyzer)
        return nullptr;

    const ColumnWithTypeAndName & elem = arguments[0];
    if (elem.type->onlyNull())
        return result_type->createColumnConst(1, UInt8(1));

    if (canContainNull(*elem.type))
        return nullptr;

    return result_type->createColumnConst(1, UInt8(0));
}

ColumnPtr FunctionIsNull::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const
{
    const ColumnWithTypeAndName & elem = arguments[0];

    if (isVariant(elem.type) || isDynamic(elem.type))
    {
        const auto & column_variant = isVariant(elem.type) ? checkAndGetColumn<ColumnVariant>(*elem.column) : checkAndGetColumn<ColumnDynamic>(*elem.column).getVariantColumn();
        const auto & discriminators = column_variant.getLocalDiscriminators();
        auto res = DataTypeUInt8().createColumn();
        auto & data = typeid_cast<ColumnUInt8 &>(*res).getData();
        data.reserve(discriminators.size());
        for (auto discr : discriminators)
            data.push_back(discr == ColumnVariant::NULL_DISCRIMINATOR);
        return res;
    }

    if (elem.type->isLowCardinalityNullable())
    {
        const auto & low_cardinality_column = checkAndGetColumn<ColumnLowCardinality>(*elem.column);
        size_t null_index = low_cardinality_column.getDictionary().getNullValueIndex();
        auto res = DataTypeUInt8().createColumn();
        auto & data = typeid_cast<ColumnUInt8 &>(*res).getData();
        data.reserve(low_cardinality_column.size());
        for (size_t i = 0; i != low_cardinality_column.size(); ++i)
            data.push_back(low_cardinality_column.getIndexAt(i) == null_index);
        return res;
    }

    if (const auto * nullable = checkAndGetColumn<ColumnNullable>(&*elem.column))
    {
        /// Merely return the embedded null map.
        return nullable->getNullMapColumnPtr();
    }

    /// Since no element is nullable, return a zero-constant column representing
    /// a zero-filled null map.
    return DataTypeUInt8().createColumnConst(elem.column->size(), 0u);
}

REGISTER_FUNCTION(IsNull)
{
    FunctionDocumentation::Description description = R"(
Checks if the argument is `NULL`.

Also see: operator [`IS NULL`](/sql-reference/operators#is_null).
    )";
    FunctionDocumentation::Syntax syntax = "isNull(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "A value of non-compound data type.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if `x` is `NULL`, otherwise `0`.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
CREATE TABLE t_null
(
  x Int32,
  y Nullable(Int32)
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO t_null VALUES (1, NULL), (2, 3);

SELECT x FROM t_null WHERE isNull(y);
        )",
        R"(
┌─x─┐
│ 1 │
└───┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Null;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionIsNull>(documentation, FunctionFactory::Case::Insensitive);
}

}
