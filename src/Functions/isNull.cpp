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
#include <Common/assert_cast.h>

#include <Functions/isNull.h>

namespace DB
{

namespace
{

template <typename IndexType>
void vectorIndexEquals(const PaddedPODArray<IndexType> & indexes, size_t null_index, PaddedPODArray<UInt8> & res)
{
    const IndexType null_position = static_cast<IndexType>(null_index);
    const IndexType * __restrict src = indexes.data();
    UInt8 * __restrict dst = res.data();
    const size_t size = indexes.size();
    for (size_t i = 0; i < size; ++i)
        dst[i] = src[i] == null_position;
}

}

FunctionPtr FunctionIsNull::create(ContextPtr)
{
    return std::make_shared<FunctionIsNull>();
}

ColumnPtr FunctionIsNull::getConstantResultForNonConstArguments(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const
{
    /// SELECT arrayFilter(x -> (x IS NULL), []) can trigger `defaultImplementationForNothing()`
    /// which will give return type Nothing. We cannot create constant column of type Nothing so return nullptr.
    if (isNothing(result_type))
        return nullptr;

    const ColumnWithTypeAndName & elem = arguments[0];
    if (elem.type->onlyNull())
        return result_type->createColumnConst(1, UInt8(1));

    if (canContainNull(*elem.type))
        return nullptr;

    return result_type->createColumnConst(1, UInt8(0));
}


ColumnPtr FunctionIsNull::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
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
        const size_t null_index = low_cardinality_column.getDictionary().getNullValueIndex();
        const IColumn & indexes = low_cardinality_column.getIndexes();
        auto res_column = ColumnUInt8::create(indexes.size());
        auto & res_data = res_column->getData();
        switch (low_cardinality_column.getSizeOfIndexType())
        {
            case sizeof(UInt8): vectorIndexEquals(assert_cast<const ColumnUInt8 &>(indexes).getData(), null_index, res_data); break;
            case sizeof(UInt16): vectorIndexEquals(assert_cast<const ColumnUInt16 &>(indexes).getData(), null_index, res_data); break;
            case sizeof(UInt32): vectorIndexEquals(assert_cast<const ColumnUInt32 &>(indexes).getData(), null_index, res_data); break;
            case sizeof(UInt64): vectorIndexEquals(assert_cast<const ColumnUInt64 &>(indexes).getData(), null_index, res_data); break;
            default: throwUnexpectedLowCardinalityIndexType(low_cardinality_column.getSizeOfIndexType());
        }
        return res_column;
    }

    if (const auto * nullable = checkAndGetColumn<ColumnNullable>(&*elem.column))
    {
        /// A null map byte only has to be non-zero to mean NULL, so it cannot be returned as the result.
        auto res_column = ColumnUInt8::create(input_rows_count);
        const auto & null_map = nullable->getNullMapData();
        auto & res_data = res_column->getData();
        for (size_t i = 0; i < input_rows_count; ++i)
            res_data[i] = null_map[i] != 0;
        return res_column;
    }

    /// Since no element is nullable, return a zero-constant column representing
    /// a zero-filled null map.
    return DataTypeUInt8().createColumnConst(elem.column->size(), 0u);
}

#if USE_EMBEDDED_COMPILER
llvm::Value *
FunctionIsNull::compileImpl(llvm::IRBuilderBase & builder, const ValuesWithType & arguments, const DataTypePtr & /*result_type*/) const
{
    auto & b = static_cast<llvm::IRBuilder<> &>(builder);
    if (arguments[0].type->isNullable())
    {
        auto * is_null = b.CreateExtractValue(arguments[0].value, {1});
        return b.CreateSelect(is_null, b.getInt8(1), b.getInt8(0));
    }
    else
        return b.getInt8(0);
}
#endif

REGISTER_FUNCTION(IsNull)
{
    FunctionDocumentation::Description description = R"(
Checks if the argument is `NULL`.

Also see: operator [`IS NULL`](/reference/operators/index#is_null).
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
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionIsNull>(documentation, FunctionFactory::Case::Insensitive);
}
};
