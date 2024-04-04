#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/getMostSubtype.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/castColumn.h>
#include <base/TypeLists.h>
#include <base/range.h>
#include <Common/HashTable/ClearableHashMap.h>
#include <Common/assert_cast.h>

namespace DB
{

class FunctionArrayLogical : public IFunction
{
public:
    FunctionArrayLogical(bool intersect_, const char *name_) : intersect(intersect_), name(name_) {}

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isSuitableForShortCircuitArgumentsExecution(
            const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes &arguments) const override;

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName &arguments, const DataTypePtr &result_type,
                size_t input_rows_count) const override;

    bool useDefaultImplementationForConstants() const override { return true; }

private:
    ContextPtr context;
    const bool intersect;
    const char *name;

    /// Initially allocate a piece of memory for 64 elements. NOTE: This is just a guess.
    static constexpr size_t
    INITIAL_SIZE_DEGREE = 6;

    struct UnpackedArrays
    {
        size_t base_rows = 0;

        struct UnpackedArray
        {
            bool is_const = false;
            const NullMap *null_map = nullptr;
            const NullMap *overflow_mask = nullptr;
            const ColumnArray::ColumnOffsets::Container *offsets = nullptr;
            const IColumn *nested_column = nullptr;
        };

        std::vector <UnpackedArray> args;
        Columns column_holders;

        UnpackedArrays() = default;
    };

    /// Cast column to data_type removing nullable if data_type hasn't.
    /// It's expected that column can represent data_type after removing some NullMap's.
    ColumnPtr castRemoveNullable(const ColumnPtr &column, const DataTypePtr &data_type) const;

    struct CastArgumentsResult
    {
        ColumnsWithTypeAndName initial;
        ColumnsWithTypeAndName casted;
    };

    static CastArgumentsResult
    castColumns(const ColumnsWithTypeAndName &arguments, const DataTypePtr &return_type,
                const DataTypePtr &return_type_with_nulls);

    UnpackedArrays
    prepareArrays(const ColumnsWithTypeAndName &columns, ColumnsWithTypeAndName &initial_columns) const;

    template<typename ValueType, typename ColumnType, bool is_numeric_column>
    static ColumnPtr execute(const UnpackedArrays &arrays, MutableColumnPtr result_data, bool intersect);

    struct NumberExecutor
    {
        const UnpackedArrays &arrays;
        const DataTypePtr &data_type;
        const bool intersect;

        ColumnPtr &result;

        NumberExecutor(const UnpackedArrays &arrays_, const DataTypePtr &data_type_, bool intersect_,
                       ColumnPtr &result_)
                : arrays(arrays_), data_type(data_type_), intersect(intersect_), result(result_) {
        }

        template<class T>
        void operator()(TypeList <T>);
    };

    struct DecimalExecutor
    {
        const UnpackedArrays &arrays;
        const DataTypePtr &data_type;
        const bool intersect;

        ColumnPtr &result;

        DecimalExecutor(const UnpackedArrays &arrays_, const DataTypePtr &data_type_, bool intersect_,
                        ColumnPtr &result_)
                : arrays(arrays_), data_type(data_type_), intersect(intersect_), result(result_) {
        }

        template<class T>
        void operator()(TypeList <T>);
    };
};

}
