#include <Columns/ColumnUnique.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/ReadBufferFromString.h>

#include <gtest/gtest.h>

#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/scope_guard_safe.h>
#include <vector>
using namespace DB;

namespace DB::ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

TEST(ColumnUnique, InsertRange)
{
    UnorderedMapWithMemoryTracking<String, size_t> ref_map;
    auto data_type = std::make_shared<DataTypeString>();
    auto column_unique = ColumnUnique<ColumnString>::create(*data_type);
    auto column_string = ColumnString::create();

    size_t num_values = 1000000;
    size_t mod_to = 1000;

    VectorWithMemoryTracking<size_t> indexes(num_values);
    for (size_t i = 0; i < num_values; ++i)
    {
        String str = toString(i % mod_to);
        column_string->insertData(str.data(), str.size());

        if (!ref_map.contains(str))
            ref_map[str] = ref_map.size();

        indexes[i]= ref_map[str];
    }

    auto idx = column_unique->uniqueInsertRangeFrom(*column_string, 0, num_values);
    ASSERT_EQ(idx->size(), num_values);

    for (size_t i = 0; i < num_values; ++i)
    {
        ASSERT_EQ(indexes[i] + 1, idx->getUInt(i)) << "Different indexes at position " << i;
    }

    const auto & nested = column_unique->getNestedColumn();
    ASSERT_EQ(nested->size(), mod_to + 1);

    for (size_t i = 0; i < mod_to; ++i)
    {
        ASSERT_EQ(std::to_string(i), nested->getDataAt(i + 1));
    }
}

TEST(ColumnUnique, InsertRangeWithOverflow)
{
    UnorderedMapWithMemoryTracking<String, size_t> ref_map;
    auto data_type = std::make_shared<DataTypeString>();
    auto column_unique = ColumnUnique<ColumnString>::create(*data_type);
    auto column_string = ColumnString::create();

    size_t num_values = 1000000;
    size_t mod_to = 1000;

    VectorWithMemoryTracking<size_t> indexes(num_values);
    for (size_t i = 0; i < num_values; ++i)
    {
        String str = toString(i % mod_to);
        column_string->insertData(str.data(), str.size());

        if (!ref_map.contains(str))
            ref_map[str] = ref_map.size();

        indexes[i]= ref_map[str];
    }

    size_t max_val = mod_to / 2;
    size_t max_dict_size = max_val + 1;
    auto idx_with_overflow = column_unique->uniqueInsertRangeWithOverflow(*column_string, 0, num_values, max_dict_size);
    auto & idx = idx_with_overflow.indexes;
    auto & add_keys = idx_with_overflow.overflowed_keys;

    ASSERT_EQ(idx->size(), num_values);

    for (size_t i = 0; i < num_values; ++i)
    {
        ASSERT_EQ(indexes[i] + 1, idx->getUInt(i)) << "Different indexes at position " << i;
    }

    const auto & nested = column_unique->getNestedColumn();
    ASSERT_EQ(nested->size(), max_dict_size);
    ASSERT_EQ(add_keys->size(), mod_to - max_val);

    for (size_t i = 0; i < max_val; ++i)
    {
        ASSERT_EQ(std::to_string(i), nested->getDataAt(i + 1));
    }

    for (size_t i = 0; i < mod_to - max_val; ++i)
    {
        ASSERT_EQ(std::to_string(max_val + i), add_keys->getDataAt(i));
    }
}

template <typename ColumnType>
void column_unique_unique_deserialize_from_arena_impl(ColumnType & column, const IDataType & data_type)
{
    size_t num_values = column.size();

    {
        /// Check serialization is reversible.
        Arena arena;
        auto column_unique_pattern = ColumnUnique<ColumnString>::create(data_type);
        auto column_unique = ColumnUnique<ColumnString>::create(data_type);
        auto idx = column_unique_pattern->uniqueInsertRangeFrom(column, 0, num_values);

        const char * pos = nullptr;
        for (size_t i = 0; i < num_values; ++i)
        {
            auto ref = column_unique_pattern->serializeValueIntoArena(idx->getUInt(i), arena, pos, nullptr);
            ReadBufferFromString in(ref);
            column_unique->uniqueDeserializeAndInsertFromArena(in, nullptr);
            ASSERT_TRUE(in.eof()) << "Deserialized data has different sizes at position " << i;

            ASSERT_EQ(column_unique_pattern->getNestedNotNullableColumn()->getDataAt(idx->getUInt(i)),
                      column_unique->getNestedNotNullableColumn()->getDataAt(idx->getUInt(i)))
                                        << "Deserialized data is different from pattern at position " << i;

        }
    }

    {
        /// Check serialization the same with ordinary column.
        Arena arena_string;
        Arena arena_lc;
        auto column_unique = ColumnUnique<ColumnString>::create(data_type);
        auto idx = column_unique->uniqueInsertRangeFrom(column, 0, num_values);

        const char * pos_string = nullptr;
        const char * pos_lc = nullptr;
        for (size_t i = 0; i < num_values; ++i)
        {
            auto ref_string = column.serializeValueIntoArena(i, arena_string, pos_string, nullptr);
            auto ref_lc = column_unique->serializeValueIntoArena(idx->getUInt(i), arena_lc, pos_lc, nullptr);
            ASSERT_EQ(ref_string, ref_lc) << "Serialized data is different from pattern at position " << i;
        }
    }
}

TEST(ColumnUnique, DeserializeFromArenaString)
{
    auto data_type = std::make_shared<DataTypeString>();
    auto column_string = ColumnString::create();

    size_t num_values = 1000000;
    size_t mod_to = 1000;

    VectorWithMemoryTracking<size_t> indexes(num_values);
    for (size_t i = 0; i < num_values; ++i)
    {
        String str = toString(i % mod_to);
        column_string->insertData(str.data(), str.size());
    }

    column_unique_unique_deserialize_from_arena_impl(*column_string, *data_type);
}

TEST(ColumnUnique, DeserializeFromArenaNullableString)
{
    auto data_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    auto column_string = ColumnString::create();
    auto null_mask = ColumnUInt8::create();

    size_t num_values = 1000000;
    size_t mod_to = 1000;

    VectorWithMemoryTracking<size_t> indexes(num_values);
    for (size_t i = 0; i < num_values; ++i)
    {
        String str = toString(i % mod_to);
        column_string->insertData(str.data(), str.size());

        null_mask->insertValue(i % 3 ? 1 : 0);
    }

    auto column = ColumnNullable::create(std::move(column_string), std::move(null_mask));
    column_unique_unique_deserialize_from_arena_impl(*column, *data_type);
}

TEST(ColumnVector, CorrectnessOfReplicate)
{
    const auto column = ColumnUInt8::create();

    column->insertValue(3);
    column->insertValue(2);
    column->insertValue(1);

    const auto empty_column = column->replicate({0, 0, 0});
    const auto * empty_column_ptr = typeid_cast<const ColumnUInt8 *>(empty_column.get());
    EXPECT_NE(empty_column_ptr, nullptr);
    EXPECT_EQ(empty_column_ptr->size(), 0);

    const auto new_column = column->replicate({1, 1, 5});
    const auto * new_column_ptr = typeid_cast<const ColumnUInt8 *>(new_column.get());
    EXPECT_NE(new_column_ptr, nullptr);
    EXPECT_EQ(new_column_ptr->size(), 5);
    const auto * it = new_column_ptr->getData().cbegin();
    for (const auto num : {3, 1, 1, 1, 1})
    {
        EXPECT_EQ(*it, num);
        ++it;
    }
}

/// Sanitizers replace `operator new`, so its allocations are not tracked and cannot hit the memory limit.
#if !defined(SANITIZER)
TEST(ColumnUnique, NullableViewIsRestoredWhenItsReconstructionHitsMemoryLimit)
{
    auto data_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    auto column_unique = ColumnUnique<ColumnString>::create(*data_type);
    column_unique->uniqueInsert(Field(String("a")));
    const size_t size = column_unique->size();

    {
        /// Drain deferred allocations so the clamp below is exact; a no-op with no thread status.
        CurrentThread::flushUntrackedMemory();
        const Int64 prev_hard_limit = total_memory_tracker.getHardLimit();
        SCOPE_EXIT_SAFE(total_memory_tracker.setHardLimit(prev_hard_limit));
        /// Let `operator new` refuse allocations of any size, as with a small
        /// `min_allocation_size_to_throw_on_memory_limit` server setting.
        const UInt64 prev_min_allocation_size_to_throw = CurrentMemoryTracker::getMinAllocationSizeBytesToThrow();
        SCOPE_EXIT_SAFE(CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(prev_min_allocation_size_to_throw));
        CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(1);

        /// The callback itself succeeds, but every allocation after it fails, so the
        /// re-creation of the nullable view after the callback hits the memory limit.
        /// The exception must propagate instead of calling `std::terminate`.
        int error_code = 0;
        try
        {
            column_unique->forEachMutableSubcolumn([](IColumn::WrappedPtr &)
            {
                total_memory_tracker.setHardLimit(total_memory_tracker.get());
            });
        }
        catch (const Exception & e)
        {
            error_code = e.code();
        }
        ASSERT_EQ(error_code, ErrorCodes::MEMORY_LIMIT_EXCEEDED);
    }

    /// The nullable view and `reverse_index` must be valid after the exception.
    ASSERT_NE(column_unique->getNestedColumn(), nullptr);
    ASSERT_EQ(column_unique->getNestedColumn()->size(), size);
    ASSERT_TRUE(column_unique->getNestedColumn()->isNullAt(column_unique->getNullValueIndex()));
    ASSERT_EQ(column_unique->uniqueInsert(Field(String("a"))), size - 1);
    ASSERT_EQ(column_unique->uniqueInsert(Field(String("b"))), size);
    ASSERT_EQ(column_unique->getNestedColumn()->size(), size + 1);
}
#endif
