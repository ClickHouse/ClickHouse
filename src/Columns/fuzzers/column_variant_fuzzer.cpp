#include <cstring>

#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnVector.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>
#include <Interpreters/Context.h>

using namespace DB;

ContextMutablePtr context;

extern "C" int LLVMFuzzerInitialize(int *, char ***)
{
    if (context)
        return true;

    static SharedContextHolder shared_context = Context::createShared();
    context = Context::createGlobal(shared_context.get());
    context->makeGlobalContext();

    MainThreadStatus::getInstance();

    return 0;
}

/// Build a small ColumnVariant with 2 variants (UInt64 and String) in a given local order.
/// local_to_global: mapping from local discriminator index to global discriminator index.
/// Inserts a few rows so there is real data to copy from.
static MutableColumnPtr buildSourceVariantColumn(const std::vector<ColumnVariant::Discriminator> & local_to_global)
{
    /// Two variants: global 0 = String, global 1 = UInt64 (sorted alphabetically: String < UInt64)
    MutableColumns nested;
    nested.push_back(ColumnString::create());
    nested.push_back(ColumnVector<UInt64>::create());

    auto col = ColumnVariant::create(std::move(nested), local_to_global);

    /// Determine local discriminators for UInt64 (global=1) and String (global=0)
    /// by reversing the provided local_to_global mapping.
    ColumnVariant::Discriminator local_string = ColumnVariant::NULL_DISCRIMINATOR;
    ColumnVariant::Discriminator local_uint64 = ColumnVariant::NULL_DISCRIMINATOR;
    for (size_t i = 0; i < local_to_global.size(); ++i)
    {
        if (local_to_global[i] == 0)
            local_string = static_cast<ColumnVariant::Discriminator>(i);
        else if (local_to_global[i] == 1)
            local_uint64 = static_cast<ColumnVariant::Discriminator>(i);
    }

    /// Insert: String "hello", UInt64 42, NULL, String "world", UInt64 100
    if (local_string != ColumnVariant::NULL_DISCRIMINATOR)
        col->insertIntoVariantFrom(0 /* global String */, *ColumnString::create(), 0);

    /// Use Field-based insertion which is safer for building test data
    col->insert(Field(String("hello")));
    col->insert(Field(UInt64(42)));
    col->insertDefault(); /// NULL
    col->insert(Field(String("world")));
    col->insert(Field(UInt64(100)));

    return col;
}

/// Build a ColumnDynamic with a few rows.
static MutableColumnPtr buildSourceDynamicColumn()
{
    auto col = ColumnDynamic::create(/*max_dynamic_types=*/4);
    col->insert(Field(UInt64(1)));
    col->insert(Field(String("foo")));
    col->insertDefault();
    col->insert(Field(UInt64(99)));
    col->insert(Field(String("bar")));
    return col;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    try
    {
        total_memory_tracker.resetCounters();
        total_memory_tracker.setHardLimit(1_GiB);
        CurrentThread::get().memory_tracker.resetCounters();
        CurrentThread::get().memory_tracker.setHardLimit(1_GiB);

        /// Need at least 10 bytes for header:
        ///   [0]    = discriminator mapping selector byte
        ///   [1..8] = row count (uint64_t, little-endian)
        ///   [9]    = start offset byte
        if (size < 10)
            return 0;

        /// Byte 0: low bit selects which local ordering the src column uses.
        ///   0 -> src local order: (String=0, UInt64=1), i.e. local_to_global = {0, 1}
        ///   1 -> src local order: (UInt64=0, String=1), i.e. local_to_global = {1, 0}
        const uint8_t mapping_selector = data[0];

        /// Bytes 1..8: requested row count, clamped to a safe range.
        uint64_t requested_rows = 0;
        memcpy(&requested_rows, data + 1, sizeof(uint64_t));
        /// Cap at 64 to keep the fuzzer fast and avoid huge allocations.
        const size_t row_count = static_cast<size_t>(requested_rows % 64);

        /// Byte 9: start offset byte (clamped later against actual column size).
        const uint8_t start_byte = data[9];

        /// -------------------------------------------------------------------
        /// Task A-1: fuzz ColumnVariant::insertRangeFrom
        /// -------------------------------------------------------------------
        {
            /// Source column: local order controlled by mapping_selector
            const std::vector<ColumnVariant::Discriminator> src_local_to_global
                = (mapping_selector & 1) ? std::vector<ColumnVariant::Discriminator>{1, 0}
                                         : std::vector<ColumnVariant::Discriminator>{0, 1};

            /// Destination column: always in global order (local_to_global = {0, 1})
            const std::vector<ColumnVariant::Discriminator> dst_local_to_global = {0, 1};

            auto src_col = buildSourceVariantColumn(src_local_to_global);
            auto dst_col = buildSourceVariantColumn(dst_local_to_global);

            const size_t src_size = src_col->size();
            if (src_size == 0)
                return 0;

            const size_t start = static_cast<size_t>(start_byte) % src_size;
            const size_t max_len = src_size - start;
            const size_t length = (row_count == 0) ? 0 : (row_count % (max_len + 1));

            dst_col->insertRangeFrom(*src_col, start, length);
        }

        /// -------------------------------------------------------------------
        /// Task A-2: fuzz ColumnVariant::insertRangeFrom with explicit mapping
        /// -------------------------------------------------------------------
        {
            /// Source has reversed local order: local 0 = UInt64 (global 1), local 1 = String (global 0)
            const std::vector<ColumnVariant::Discriminator> src_local_to_global = {1, 0};
            /// Destination has normal local order: local 0 = String (global 0), local 1 = UInt64 (global 1)
            const std::vector<ColumnVariant::Discriminator> dst_local_to_global = {0, 1};

            auto src_col_raw = buildSourceVariantColumn(src_local_to_global);
            auto dst_col_raw = buildSourceVariantColumn(dst_local_to_global);

            auto & src_variant = assert_cast<ColumnVariant &>(*src_col_raw);
            auto & dst_variant = assert_cast<ColumnVariant &>(*dst_col_raw);

            const size_t src_size = src_variant.size();
            if (src_size == 0)
                return 0;

            const size_t start = static_cast<size_t>(start_byte) % src_size;
            const size_t max_len = src_size - start;
            const size_t length = (row_count == 0) ? 0 : (row_count % (max_len + 1));

            /// global_discriminators_mapping: src global discr i -> dst global discr i
            /// Both columns share the same 2 global types, so identity mapping is correct.
            const std::vector<ColumnVariant::Discriminator> global_mapping = {0, 1};
            dst_variant.insertRangeFrom(
                src_variant, start, length, global_mapping, ColumnVariant::NULL_DISCRIMINATOR);
        }

        /// -------------------------------------------------------------------
        /// Task A-3: fuzz ColumnDynamic::insertRangeFrom
        /// -------------------------------------------------------------------
        {
            auto src_dyn = buildSourceDynamicColumn();
            auto dst_dyn = ColumnDynamic::create(/*max_dynamic_types=*/4);

            const size_t src_size = src_dyn->size();
            if (src_size == 0)
                return 0;

            const size_t start = static_cast<size_t>(start_byte) % src_size;
            const size_t max_len = src_size - start;
            const size_t length = (row_count == 0) ? 0 : (row_count % (max_len + 1));

            dst_dyn->insertRangeFrom(*src_dyn, start, length);
        }
    }
    catch (...)
    {
    }

    return 0;
}
