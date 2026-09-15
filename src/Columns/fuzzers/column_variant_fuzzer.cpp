#include <cstring>

#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnVector.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/MemoryTracker.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Interpreters/Context.h>

using namespace DB;

using DiscriminatorVec = VectorWithMemoryTracking<ColumnVariant::Discriminator>;

ContextMutablePtr context;

extern "C" int LLVMFuzzerInitialize(int *, char ***);
extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size);

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

/// Build a small ColumnVariant with 2 variants (String and UInt64) using a given local order.
/// The global type set is fixed: global 0 = String, global 1 = UInt64 (sorted alphabetically: String < UInt64).
/// local_to_global: mapping from local discriminator index to global discriminator index.
/// Inserts a few rows so there is real data to copy from.
static MutableColumnPtr buildSourceVariantColumn(const DiscriminatorVec & local_to_global)
{
    /// Build the nested columns in *local* order so the requested (possibly non-identity) local↔global
    /// mapping is preserved. Passing non-empty variants together with an explicit mapping to the
    /// empty-columns create() overload would reorder variants into global order and install the identity
    /// mapping, which silently defeats the intent of exercising a reversed local order.
    MutableColumns nested;
    nested.reserve(local_to_global.size());
    for (const auto global_discr : local_to_global)
    {
        if (global_discr == 0)
            nested.push_back(ColumnString::create()); /// global 0 = String
        else
            nested.push_back(ColumnVector<UInt64>::create()); /// global 1 = UInt64
    }

    /// Pass an empty local_discriminators column so this create() overload keeps the mapping as given
    /// (variants stay in local order) instead of normalizing it to identity.
    auto col = ColumnVariant::create(ColumnVariant::ColumnDiscriminators::create(), std::move(nested), local_to_global);

    /// Insert rows via the Field-based API, which routes each value to its variant and records the
    /// matching local discriminator, so the local↔global mapping is honoured automatically.
    col->insert(Field(String("hello")));
    col->insert(Field(UInt64(42)));
    col->insertDefault(); /// NULL
    col->insert(Field(String("world")));
    col->insert(Field(UInt64(100)));

    return col;
}

/// Build a `ColumnDynamic` with a few rows.
/// When `overflow` is set, more distinct types are inserted than `max_dynamic_types` allows, so the
/// surplus ones end up in the shared variant. That is required to reach the combine/extract branch of
/// `ColumnDynamic::insertRangeFrom`, which decodes the type tag out of the shared blob and rewrites
/// discriminators and offsets; with only `UInt8`, `String` and `NULL` the shared variant stays empty
/// and that branch is never taken.
static MutableColumnPtr buildSourceDynamicColumn(size_t max_dynamic_types, bool overflow)
{
    auto col = ColumnDynamic::create(max_dynamic_types);
    col->insert(Field(UInt64(1)));
    col->insert(Field(String("foo")));
    col->insertDefault();
    col->insert(Field(UInt64(99)));
    col->insert(Field(String("bar")));

    if (overflow)
    {
        col->insert(Field(Float64(1.5)));
        col->insert(Field(Array{Field(UInt64(1)), Field(UInt64(2))}));
        col->insert(Field(Tuple{Field(UInt64(7))}));
    }

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

        /// Byte 0, bit 0: selects which local ordering the src column uses.
        /// Bits 1 and 2 drive the `ColumnDynamic` shared-variant shape, see task A-3 below.
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
            const DiscriminatorVec src_local_to_global
                = (mapping_selector & 1) ? DiscriminatorVec{1, 0}
                                         : DiscriminatorVec{0, 1};

            /// Destination column: always in global order (local_to_global = {0, 1})
            const DiscriminatorVec dst_local_to_global = {0, 1};

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
            const DiscriminatorVec src_local_to_global = {1, 0};
            /// Destination has normal local order: local 0 = String (global 0), local 1 = UInt64 (global 1)
            const DiscriminatorVec dst_local_to_global = {0, 1};

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
            const DiscriminatorVec global_mapping = {0, 1};
            dst_variant.insertRangeFrom(
                src_variant, start, length, global_mapping, ColumnVariant::NULL_DISCRIMINATOR);
        }

        /// -------------------------------------------------------------------
        /// Task A-3: fuzz ColumnDynamic::insertRangeFrom
        /// -------------------------------------------------------------------
        {
            /// Bit 1 selects whether the source overflows into its shared variant, bit 2 selects whether the
            /// destination has enough free slots to promote those overflowed types back to regular variants.
            /// The interesting combination is an overflowing source plus a roomy destination: the fast path
            /// at the top of `ColumnDynamic::insertRangeFrom` is skipped only when the source shared variant
            /// is non-empty, and only a destination that can still add variants reaches the extract branch.
            const bool src_overflows = (mapping_selector & 2) != 0;
            const size_t src_max_dynamic_types = src_overflows ? 2 : 4;
            const size_t dst_max_dynamic_types = (mapping_selector & 4) ? 8 : src_max_dynamic_types;

            auto src_dyn = buildSourceDynamicColumn(src_max_dynamic_types, src_overflows);
            auto dst_dyn = ColumnDynamic::create(dst_max_dynamic_types);

            const size_t src_size = src_dyn->size();
            if (src_size == 0)
                return 0;

            const size_t start = static_cast<size_t>(start_byte) % src_size;
            const size_t max_len = src_size - start;
            const size_t length = (row_count == 0) ? 0 : (row_count % (max_len + 1));

            dst_dyn->insertRangeFrom(*src_dyn, start, length);
        }
    }
    catch (...) // Ok: fuzzer intentionally discards all exceptions
    {
    }

    return 0;
}
