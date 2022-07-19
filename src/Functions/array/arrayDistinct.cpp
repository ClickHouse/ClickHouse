#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Common/HashTable/ClearableHashSet.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


/// Find different elements in an array.
class FunctionArrayDistinct : public IFunction
{
public:
    static constexpr auto name = "arrayDistinct";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionArrayDistinct>();
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
        if (!array_type)
            throw Exception("Argument for function " + getName() + " must be array but it "
                " has type " + arguments[0]->getName() + ".",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        auto nested_type = removeNullable(array_type->getNestedType());

        return std::make_shared<DataTypeArray>(nested_type);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;

private:
    /// Initially allocate a piece of memory for 512 elements. NOTE: This is just a guess.
    static constexpr size_t INITIAL_SIZE_DEGREE = 9;

    template <typename T>
    static bool executeNumber(
        const IColumn & src_data,
        const ColumnArray::Offsets & src_offsets,
        IColumn & res_data_col,
        ColumnArray::Offsets & res_offsets,
        const ColumnNullable * nullable_col);

    static bool executeString(
        const IColumn & src_data,
        const ColumnArray::Offsets & src_offsets,
        IColumn & res_data_col,
        ColumnArray::Offsets & res_offsets,
        const ColumnNullable * nullable_col);

    static void executeHashed(
        const IColumn & src_data,
        const ColumnArray::Offsets & src_offsets,
        IColumn & res_data_col,
        ColumnArray::Offsets & res_offsets,
        const ColumnNullable * nullable_col);
};


ColumnPtr FunctionArrayDistinct::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const
{
    ColumnPtr array_ptr = arguments[0].column;
    const ColumnArray * array = checkAndGetColumn<ColumnArray>(array_ptr.get());

    const auto & return_type = result_type;

    auto res_ptr = return_type->createColumn();
    ColumnArray & res = assert_cast<ColumnArray &>(*res_ptr);

    const IColumn & src_data = array->getData();
    const ColumnArray::Offsets & offsets = array->getOffsets();

    IColumn & res_data = res.getData();
    ColumnArray::Offsets & res_offsets = res.getOffsets();

    const ColumnNullable * nullable_col = checkAndGetColumn<ColumnNullable>(src_data);

    const IColumn * inner_col;

    if (nullable_col)
    {
        inner_col = &nullable_col->getNestedColumn();
    }
    else
    {
        inner_col = &src_data;
    }

    if (!(executeNumber<UInt8>(*inner_col, offsets, res_data, res_offsets, nullable_col)
        || executeNumber<UInt16>(*inner_col, offsets, res_data, res_offsets, nullable_col)
        || executeNumber<UInt32>(*inner_col, offsets, res_data, res_offsets, nullable_col)
        || executeNumber<UInt64>(*inner_col, offsets, res_data, res_offsets, nullable_col)
        || executeNumber<Int8>(*inner_col, offsets, res_data, res_offsets, nullable_col)
        || executeNumber<Int16>(*inner_col, offsets, res_data, res_offsets, nullable_col)
        || executeNumber<Int32>(*inner_col, offsets, res_data, res_offsets, nullable_col)
        || executeNumber<Int64>(*inner_col, offsets, res_data, res_offsets, nullable_col)
        || executeNumber<Float32>(*inner_col, offsets, res_data, res_offsets, nullable_col)
        || executeNumber<Float64>(*inner_col, offsets, res_data, res_offsets, nullable_col)
        || executeString(*inner_col, offsets, res_data, res_offsets, nullable_col)))
        executeHashed(*inner_col, offsets, res_data, res_offsets, nullable_col);

    return res_ptr;
}

template <typename T>
bool FunctionArrayDistinct::executeNumber(
    const IColumn & src_data,
    const ColumnArray::Offsets & src_offsets,
    IColumn & res_data_col,
    ColumnArray::Offsets & res_offsets,
    const ColumnNullable * nullable_col)
{
    const ColumnVector<T> * src_data_concrete = checkAndGetColumn<ColumnVector<T>>(&src_data);

    if (!src_data_concrete)
    {
        return false;
    }

    const PaddedPODArray<T> & values = src_data_concrete->getData();
    PaddedPODArray<T> & res_data = typeid_cast<ColumnVector<T> &>(res_data_col).getData();

    const PaddedPODArray<UInt8> * src_null_map = nullptr;

    if (nullable_col)
        src_null_map = &nullable_col->getNullMapData();

    using Set = ClearableHashSetWithStackMemory<T, DefaultHash<T>,
        INITIAL_SIZE_DEGREE>;

    Set set;

    ColumnArray::Offset prev_src_offset = 0;
    ColumnArray::Offset res_offset = 0;

    for (auto curr_src_offset : src_offsets)
    {
        set.clear();

        for (ColumnArray::Offset j = prev_src_offset; j < curr_src_offset; ++j)
        {
            if (nullable_col && (*src_null_map)[j])
                continue;

            if (!set.find(values[j]))
            {
                res_data.emplace_back(values[j]);
                set.insert(values[j]);
            }
        }

        res_offset += set.size();
        res_offsets.emplace_back(res_offset);

        prev_src_offset = curr_src_offset;
    }
    return true;
}

bool FunctionArrayDistinct::executeString(
    const IColumn & src_data,
    const ColumnArray::Offsets & src_offsets,
    IColumn & res_data_col,
    ColumnArray::Offsets & res_offsets,
    const ColumnNullable * nullable_col)
{
    const ColumnString * src_data_concrete = checkAndGetColumn<ColumnString>(&src_data);

    if (!src_data_concrete)
        return false;

    ColumnString & res_data_column_string = typeid_cast<ColumnString &>(res_data_col);

    using Set = ClearableHashSetWithStackMemory<StringRef, StringRefHash,
        INITIAL_SIZE_DEGREE>;

    const PaddedPODArray<UInt8> * src_null_map = nullptr;

    if (nullable_col)
        src_null_map = &nullable_col->getNullMapData();

    Set set;

    ColumnArray::Offset prev_src_offset = 0;
    ColumnArray::Offset res_offset = 0;

    for (auto curr_src_offset : src_offsets)
    {
        set.clear();

        for (ColumnArray::Offset j = prev_src_offset; j < curr_src_offset; ++j)
        {
            if (nullable_col && (*src_null_map)[j])
                continue;

            StringRef str_ref = src_data_concrete->getDataAt(j);

            if (!set.find(str_ref))
            {
                set.insert(str_ref);
                res_data_column_string.insertData(str_ref.data, str_ref.size);
            }
        }

        res_offset += set.size();
        res_offsets.emplace_back(res_offset);

        prev_src_offset = curr_src_offset;
    }
    return true;
}

void FunctionArrayDistinct::executeHashed(
    const IColumn & src_data,
    const ColumnArray::Offsets & src_offsets,
    IColumn & res_data_col,
    ColumnArray::Offsets & res_offsets,
    const ColumnNullable * nullable_col)
{
    using Set = ClearableHashSetWithStackMemory<UInt128, UInt128TrivialHash,
        INITIAL_SIZE_DEGREE>;

    const PaddedPODArray<UInt8> * src_null_map = nullptr;

    if (nullable_col)
        src_null_map = &nullable_col->getNullMapData();

    Set set;

    ColumnArray::Offset prev_src_offset = 0;
    ColumnArray::Offset res_offset = 0;

    for (auto curr_src_offset : src_offsets)
    {
        set.clear();

        for (ColumnArray::Offset j = prev_src_offset; j < curr_src_offset; ++j)
        {
            if (nullable_col && (*src_null_map)[j])
                continue;

            UInt128 hash;
            SipHash hash_function;
            src_data.updateHashWithValue(j, hash_function);
            hash_function.get128(hash);

            if (!set.find(hash))
            {
                set.insert(hash);
                res_data_col.insertFrom(src_data, j);
            }
        }

        res_offset += set.size();
        res_offsets.emplace_back(res_offset);

        prev_src_offset = curr_src_offset;
    }
}


void registerFunctionArrayDistinct(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayDistinct>();
}

}
