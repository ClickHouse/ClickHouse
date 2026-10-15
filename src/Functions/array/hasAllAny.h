#pragma once

#include <base/range.h>

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <Functions/LowCardinalityExecutionHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/getLeastSupertype.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Interpreters/castColumn.h>
#include <Common/typeid_cast.h>
#include <Common/VectorWithMemoryTracking.h>

#include <ranges>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


class FunctionArrayHasAllAny : public IFunction
{
public:
    FunctionArrayHasAllAny(GatherUtils::ArraySearchType search_type_, const char * name_)
        : search_type(search_type_), name(name_) {}

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }
    size_t getNumberOfArguments() const override { return 2; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    /// FunctionWithLowCardinalityFastPath calls the base ColumnsWithTypeAndName overload by qualified name.
    using IFunction::getReturnTypeImpl;
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        for (auto i : collections::range(0, arguments.size()))
        {
            const auto * array_type = typeid_cast<const DataTypeArray *>(arguments[i].get());
            if (!array_type)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "Argument {} for function {} must be an array but it has type {}.",
                                i, getName(), arguments[i]->getName());
        }

        return std::make_shared<DataTypeUInt8>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        size_t num_args = arguments.size();

        DataTypePtr common_type
            = getLeastSupertype(DataTypes{std::from_range_t{}, arguments | std::views::transform([](auto & elem) { return elem.type; })});

        Columns preprocessed_columns(num_args);
        for (size_t i = 0; i < num_args; ++i)
            preprocessed_columns[i] = castColumn(arguments[i], common_type);

        VectorWithMemoryTracking<std::unique_ptr<GatherUtils::IArraySource>> sources;

        for (auto & argument_column : preprocessed_columns)
        {
            bool is_const = false;

            if (const auto * argument_column_const = typeid_cast<const ColumnConst *>(argument_column.get()))
            {
                is_const = true;
                argument_column = argument_column_const->getDataColumnPtr();
            }

            if (const auto * argument_column_array = typeid_cast<const ColumnArray *>(argument_column.get()))
                sources.emplace_back(GatherUtils::createArraySource(*argument_column_array, is_const, input_rows_count));
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Arguments for function {} must be arrays.", getName());
        }

        auto result_column = ColumnUInt8::create(input_rows_count);
        auto * result_column_ptr = typeid_cast<ColumnUInt8 *>(result_column.get());
        GatherUtils::sliceHas(*sources[0], *sources[1], search_type, *result_column_ptr);

        return result_column;
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    /// Hook called by FunctionWithLowCardinalityFastPath (FunctionLowCardinalityFastPath.h); nullptr declines it.
    ColumnPtr tryExecuteLowCardinality(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const
    {
        if (search_type != GatherUtils::ArraySearchType::All && search_type != GatherUtils::ArraySearchType::Any)
            return nullptr;

        const auto * haystack_type = checkAndGetDataType<DataTypeArray>(arguments[0].type.get());
        const auto * needle_type = checkAndGetDataType<DataTypeArray>(arguments[1].type.get());
        if (!haystack_type || !needle_type)
            return nullptr;

        const auto * haystack_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
        if (!haystack_array)
            return nullptr;

        const auto * haystack_elements = checkAndGetColumn<ColumnLowCardinality>(&haystack_array->getData());
        if (!haystack_elements || haystack_elements->nestedIsNullable())
            return nullptr;

        const auto * needle_array = checkAndGetColumnConstData<ColumnArray>(arguments[1].column.get());
        if (!needle_array)
            return nullptr;

        const auto dictionary_value_type = recursiveRemoveLowCardinality(haystack_type->getNestedType());
        const auto needle_element_type = recursiveRemoveLowCardinality(needle_type->getNestedType());
        if (dictionary_value_type->isNullable() || needle_element_type->isNullable())
            return nullptr;

        /// MergeTreeIndexBloomFilter hashes a hasAll/hasAny constant cast to the least supertype to decide
        /// granule pruning, which only an already equal element type coerces to by dictionary lookup.
        if (!dictionary_value_type->equals(*needle_element_type))
            return nullptr;

        /// A dictionary is unique by bytes while the general path compares values, and NaN equals no value:
        /// a needle resolved to a NaN entry would report a match that the comparison does not make.
        if (isFloat(dictionary_value_type))
            return nullptr;

        const IColumn & needle_elements = needle_array->getData();
        size_t needle_size = needle_elements.size();
        if (needle_size == 0 || needle_size > MAX_NEEDLES_FOR_FAST_PATH)
            return nullptr;

        size_t dictionary_size = haystack_elements->getDictionary().size();
        if (dictionary_size == 0 || dictionary_size > MAX_DICTIONARY_SIZE_FOR_FAST_PATH)
            return nullptr;

        PaddedPODArray<UInt8> slot_of_dictionary_index(dictionary_size, 0);
        size_t distinct_needles = 0;
        for (size_t i = 0; i != needle_size; ++i)
        {
            UInt64 dictionary_index = 0;
            /// A dictionary reports a value it does not hold either as absent, or as the position the value
            /// would be inserted at, which is one past its last entry, so both answers mean absent here.
            bool present = LowCardinalityExecutionHelpers::dictionaryIndexForConstant(
                               *haystack_elements, needle_elements.cut(i, 1), needle_element_type, dictionary_value_type, dictionary_index)
                && dictionary_index < dictionary_size;

            if (!present)
            {
                /// A block's dictionary holds every value its rows reference, so absent from it is absent from every row.
                if (search_type == GatherUtils::ArraySearchType::All)
                    return ColumnUInt8::create(input_rows_count, UInt8{0});
                continue;
            }

            if (!slot_of_dictionary_index[dictionary_index])
                slot_of_dictionary_index[dictionary_index] = static_cast<UInt8>(++distinct_needles);
        }

        if (distinct_needles == 0)
            return ColumnUInt8::create(input_rows_count, UInt8{0});

        UInt64 full_mask
            = distinct_needles == MAX_NEEDLES_FOR_FAST_PATH ? ~UInt64(0) : (UInt64(1) << distinct_needles) - 1;

        auto result = ColumnUInt8::create(input_rows_count);
        bool searched = search_type == GatherUtils::ArraySearchType::All
            ? searchByIndexType<true>(*haystack_elements, *haystack_array, slot_of_dictionary_index, full_mask, input_rows_count, result->getData())
            : searchByIndexType<false>(*haystack_elements, *haystack_array, slot_of_dictionary_index, full_mask, input_rows_count, result->getData());

        if (!searched)
            return nullptr;

        return result;
    }

private:
    static constexpr size_t MAX_NEEDLES_FOR_FAST_PATH = 64;
    static constexpr size_t MAX_DICTIONARY_SIZE_FOR_FAST_PATH = 1ULL << 16;

    template <bool is_all, typename IndexType>
    static void searchDictionaryIndexes(
        const PaddedPODArray<IndexType> & indexes,
        const ColumnArray::Offsets & offsets,
        const PaddedPODArray<UInt8> & slot_of_dictionary_index,
        UInt64 full_mask,
        size_t input_rows_count,
        PaddedPODArray<UInt8> & result)
    {
        for (size_t row = 0; row != input_rows_count; ++row)
        {
            UInt64 mask = 0;
            for (size_t i = offsets[ssize_t(row) - 1], end = offsets[row]; i != end; ++i)
            {
                UInt8 slot = slot_of_dictionary_index[indexes[i]];
                if (!slot)
                    continue;

                if constexpr (is_all)
                {
                    mask |= UInt64(1) << (slot - 1);
                    if (mask == full_mask)
                        break;
                }
                else
                {
                    mask = 1;
                    break;
                }
            }

            if constexpr (is_all)
                result[row] = mask == full_mask;
            else
                result[row] = mask != 0;
        }
    }

    template <bool is_all>
    static bool searchByIndexType(
        const ColumnLowCardinality & elements,
        const ColumnArray & array,
        const PaddedPODArray<UInt8> & slot_of_dictionary_index,
        UInt64 full_mask,
        size_t input_rows_count,
        PaddedPODArray<UInt8> & result)
    {
        const IColumn & indexes = elements.getIndexes();
        const ColumnArray::Offsets & offsets = array.getOffsets();

        if (const auto * indexes_uint8 = typeid_cast<const ColumnUInt8 *>(&indexes))
            searchDictionaryIndexes<is_all>(indexes_uint8->getData(), offsets, slot_of_dictionary_index, full_mask, input_rows_count, result);
        else if (const auto * indexes_uint16 = typeid_cast<const ColumnUInt16 *>(&indexes))
            searchDictionaryIndexes<is_all>(indexes_uint16->getData(), offsets, slot_of_dictionary_index, full_mask, input_rows_count, result);
        else if (const auto * indexes_uint32 = typeid_cast<const ColumnUInt32 *>(&indexes))
            searchDictionaryIndexes<is_all>(indexes_uint32->getData(), offsets, slot_of_dictionary_index, full_mask, input_rows_count, result);
        else if (const auto * indexes_uint64 = typeid_cast<const ColumnUInt64 *>(&indexes))
            searchDictionaryIndexes<is_all>(indexes_uint64->getData(), offsets, slot_of_dictionary_index, full_mask, input_rows_count, result);
        else
            return false;

        return true;
    }

    GatherUtils::ArraySearchType search_type;
    const char * name;
};

}
