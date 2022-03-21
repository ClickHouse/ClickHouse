#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>

#include "FunctionArrayMapped.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

enum class ArrayFirstLastStrategy : uint8_t
{
    First,
    Last
};

enum class ArrayFirstLastElementNotExistsStrategy : uint8_t
{
    Default,
    Null
};

template <ArrayFirstLastStrategy strategy, ArrayFirstLastElementNotExistsStrategy element_not_exists_strategy>
struct ArrayFirstLastImpl
{
    using column_type = ColumnArray;
    using data_type = DataTypeArray;

    static bool needBoolean() { return false; }
    static bool needExpression() { return true; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & /*expression_return*/, const DataTypePtr & array_element)
    {
        if constexpr (element_not_exists_strategy == ArrayFirstLastElementNotExistsStrategy::Null)
            return makeNullable(array_element);

        return array_element;
    }

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
        const auto * column_filter = typeid_cast<const ColumnUInt8 *>(&*mapped);

        if (!column_filter)
        {
            const auto * column_filter_const = checkAndGetColumnConst<ColumnUInt8>(&*mapped);

            if (!column_filter_const)
                throw Exception("Unexpected type of filter column", ErrorCodes::ILLEGAL_COLUMN);

            if (column_filter_const->getValue<UInt8>())
            {
                const auto & offsets = array.getOffsets();
                const auto & data = array.getData();
                auto out = data.cloneEmpty();
                out->reserve(data.size());

                size_t offsets_size = offsets.size();

                ColumnUInt8::MutablePtr col_null_map_to;
                ColumnUInt8::Container * vec_null_map_to = nullptr;

                if constexpr (element_not_exists_strategy == ArrayFirstLastElementNotExistsStrategy::Null)
                {
                    col_null_map_to = ColumnUInt8::create(offsets_size, false);
                    vec_null_map_to = &col_null_map_to->getData();
                }

                for (size_t offset_index = 0; offset_index < offsets_size; ++offset_index)
                {
                    size_t start_offset = offsets[offset_index - 1];
                    size_t end_offset = offsets[offset_index];

                    if (end_offset > start_offset)
                    {
                        if constexpr (strategy == ArrayFirstLastStrategy::First)
                            out->insert(data[start_offset]);
                        else
                            out->insert(data[end_offset - 1]);
                    }
                    else
                    {
                        out->insertDefault();

                        if constexpr (element_not_exists_strategy == ArrayFirstLastElementNotExistsStrategy::Null)
                            (*vec_null_map_to)[offset_index] = true;
                    }
                }

                if constexpr (element_not_exists_strategy == ArrayFirstLastElementNotExistsStrategy::Null)
                    return ColumnNullable::create(std::move(out), std::move(col_null_map_to));

                return out;
            }
            else
            {
                auto out = array.getData().cloneEmpty();
                out->insertManyDefaults(array.size());

                if constexpr (element_not_exists_strategy == ArrayFirstLastElementNotExistsStrategy::Null)
                {
                    auto col_null_map_to = ColumnUInt8::create(out->size(), true);
                    return ColumnNullable::create(std::move(out), std::move(col_null_map_to));
                }

                return out;
            }
        }

        const auto & filter = column_filter->getData();
        const auto & offsets = array.getOffsets();
        const auto & data = array.getData();
        auto out = data.cloneEmpty();
        out->reserve(data.size());

        size_t offsets_size = offsets.size();

        ColumnUInt8::MutablePtr col_null_map_to;
        ColumnUInt8::Container * vec_null_map_to = nullptr;

        if constexpr (element_not_exists_strategy == ArrayFirstLastElementNotExistsStrategy::Null)
        {
            col_null_map_to = ColumnUInt8::create(offsets_size, false);
            vec_null_map_to = &col_null_map_to->getData();
        }

        for (size_t offset_index = 0; offset_index < offsets_size; ++offset_index)
        {
            size_t start_offset = offsets[offset_index - 1];
            size_t end_offset = offsets[offset_index];

            bool exists = false;

            if constexpr (strategy == ArrayFirstLastStrategy::First)
            {
                for (; start_offset != end_offset; ++start_offset)
                {
                    if (filter[start_offset])
                    {
                        out->insert(data[start_offset]);
                        exists = true;
                        break;
                    }
                }
            }
            else
            {
                for (; end_offset != start_offset; --end_offset)
                {
                    if (filter[end_offset - 1])
                    {
                        out->insert(data[end_offset - 1]);
                        exists = true;
                        break;
                    }
                }
            }

            if (!exists)
            {
                out->insertDefault();

                if constexpr (element_not_exists_strategy == ArrayFirstLastElementNotExistsStrategy::Null)
                    (*vec_null_map_to)[offset_index] = true;
            }
        }

        if constexpr (element_not_exists_strategy == ArrayFirstLastElementNotExistsStrategy::Null)
            return ColumnNullable::create(std::move(out), std::move(col_null_map_to));

        return out;
    }
};

struct NameArrayFirst { static constexpr auto name = "arrayFirst"; };
using ArrayFirstImpl = ArrayFirstLastImpl<ArrayFirstLastStrategy::First, ArrayFirstLastElementNotExistsStrategy::Default>;
using FunctionArrayFirst = FunctionArrayMapped<ArrayFirstImpl, NameArrayFirst>;

struct NameArrayFirstOrNull { static constexpr auto name = "arrayFirstOrNull"; };
using ArrayFirstOrNullImpl = ArrayFirstLastImpl<ArrayFirstLastStrategy::First, ArrayFirstLastElementNotExistsStrategy::Null>;
using FunctionArrayFirstOrNull = FunctionArrayMapped<ArrayFirstOrNullImpl, NameArrayFirstOrNull>;

struct NameArrayLast { static constexpr auto name = "arrayLast"; };
using ArrayLastImpl = ArrayFirstLastImpl<ArrayFirstLastStrategy::Last, ArrayFirstLastElementNotExistsStrategy::Default>;
using FunctionArrayLast = FunctionArrayMapped<ArrayLastImpl, NameArrayLast>;

struct NameArrayLastOrNull { static constexpr auto name = "arrayLastOrNull"; };
using ArrayLastOrNullImpl = ArrayFirstLastImpl<ArrayFirstLastStrategy::Last, ArrayFirstLastElementNotExistsStrategy::Null>;
using FunctionArrayLastOrNull = FunctionArrayMapped<ArrayLastOrNullImpl, NameArrayLastOrNull>;

void registerFunctionArrayFirst(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayFirst>();
    factory.registerFunction<FunctionArrayFirstOrNull>();
    factory.registerFunction<FunctionArrayLast>();
    factory.registerFunction<FunctionArrayLastOrNull>();
}

}

