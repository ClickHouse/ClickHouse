#include <benchmark/benchmark.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Common/ColumnsHashing.h>

#include <pcg_random.hpp>
#include <Common/randomSeed.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/HashTable/TwoLevelHashMap.h>

using namespace DB;
using namespace DB::ColumnsHashing;


template <typename Value, typename Mapped, bool nullable, bool prealloc>
struct HashMethodSerializedNew
    : public columns_hashing_impl::HashMethodBase<HashMethodSerializedNew<Value, Mapped, nullable, prealloc>, Value, Mapped, false>
{
    using Self = HashMethodSerializedNew<Value, Mapped, nullable, prealloc>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, false>;

    static HashMethodContextPtr createContext(const HashMethodContextSettings & settings)
    {
        return std::make_shared<HashMethodSerializedContext>(settings);
    }

    static constexpr bool has_cheap_key_calculation = false;

    struct Columns
    {
        template <typename ColT>
        using ColumnWithNullMap = std::tuple<const ColT *, const UInt8 *, size_t>;

        explicit Columns(const ColumnRawPtrs & key_columns_, bool optimize_)
            : optimize(optimize_)
        {
            size_t pos = 0;
            for (const auto * key_column : key_columns_)
            {
                const UInt8 * null_map = nullptr;
                if constexpr (nullable)
                {
                    if (const auto * nullable_column = typeid_cast<const ColumnNullable *>(key_column))
                    {
                        key_column = nullable_column->getNestedColumnPtr().get();
                        null_map = nullable_column->getNullMapData().data();
                    }
                }

                if (optimize && typeid_cast<const ColumnString *>(key_column))
                    string_key_columns.emplace_back(static_cast<const ColumnString *>(key_column), null_map, pos++);
                else if (const auto * ptr = dynamic_cast<const ColumnFixedSizeHelper *>(key_column); ptr && optimize)
                    fixed_size_key_columns.emplace_back(ptr, null_map, pos++);
                else
                    other_key_columns.emplace_back(key_column, null_map, pos++);
            }
        }

        bool optimize = true;
        std::vector<ColumnWithNullMap<ColumnString>> string_key_columns;
        std::vector<ColumnWithNullMap<ColumnFixedSizeHelper>> fixed_size_key_columns;
        std::vector<ColumnWithNullMap<IColumn>> other_key_columns;
    };

    PaddedPODArray<UInt64> row_sizes;
    Columns key_columns;
    bool optimize = true;
    IColumn::SerializationSettings serialization_settings;

    HashMethodSerializedNew(
        const ColumnRawPtrs & key_columns_, const Sizes & /*key_sizes*/, const HashMethodContextPtr & context, bool optimize_)
        : key_columns(key_columns_, optimize_)
        , optimize(optimize_)
    {
        const auto * hash_serialized_context = typeid_cast<const HashMethodSerializedContext *>(context.get());
        if (!hash_serialized_context)
        {
            const auto & cached_val = *context;
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Invalid type for HashMethodSerialized context: {}", demangle(typeid(cached_val).name()));
        }

        serialization_settings.serialize_string_with_zero_byte = hash_serialized_context->settings.serialize_string_with_zero_byte;

        if constexpr (prealloc)
        {
            for (const auto & [key_column, null_map, _] : key_columns.string_key_columns)
                key_column->collectSerializedValueSizes(row_sizes, null_map, &serialization_settings);
            for (const auto & [key_column, null_map, _] : key_columns.fixed_size_key_columns)
                key_column->collectSerializedValueSizes(row_sizes, null_map, &serialization_settings);
            for (const auto & [key_column, null_map, _] : key_columns.other_key_columns)
                key_column->collectSerializedValueSizes(row_sizes, null_map, &serialization_settings);
        }
    }

    friend class columns_hashing_impl::HashMethodBase<Self, Value, Mapped, false>;

    ALWAYS_INLINE SerializedKeyHolder getKeyHolder(size_t row, Arena & pool) const
    {
        if constexpr (prealloc)
        {
            const char * begin = nullptr;

            char * memory = pool.allocContinue(row_sizes[row], begin);
            std::string_view key(memory, row_sizes[row]);
            for (const auto & [key_column, null_map, _] : key_columns.string_key_columns)
            {
                if constexpr (nullable)
                    memory = key_column->serializeValueIntoMemoryWithNull(row, memory, null_map, &serialization_settings);
                else
                    memory = key_column->serializeValueIntoMemory(row, memory, &serialization_settings);
            }
            for (const auto & [key_column, null_map, _] : key_columns.fixed_size_key_columns)
            {
                if constexpr (nullable)
                    memory = key_column->serializeValueIntoMemoryWithNull(row, memory, null_map, &serialization_settings);
                else
                    memory = key_column->serializeValueIntoMemory(row, memory, &serialization_settings);
            }
            for (const auto & [key_column, null_map, _] : key_columns.other_key_columns)
            {
                if constexpr (nullable)
                    memory = key_column->serializeValueIntoMemoryWithNull(row, memory, null_map, &serialization_settings);
                else
                    memory = key_column->serializeValueIntoMemory(row, memory, &serialization_settings);
            }

            return SerializedKeyHolder{key, pool};
        }
        else
        {
            const char * begin = nullptr;

            size_t sum_size = 0;
            // for (const auto & [key_column, null_map, _] : key_columns.string_key_columns)
            for (size_t i = 0; i < 2; ++i)
            {
                const auto & [key_column, null_map, _] = key_columns.string_key_columns[i];
                if constexpr (nullable)
                    sum_size += key_column->serializeValueIntoArenaWithNull(row, pool, begin, null_map, &serialization_settings).size();
                else
                    sum_size += key_column->serializeValueIntoArena(row, pool, begin, &serialization_settings).size();
            }
            for (const auto & [key_column, null_map, _] : key_columns.fixed_size_key_columns)
            {
                if constexpr (nullable)
                    sum_size += key_column->serializeValueIntoArenaWithNull(row, pool, begin, null_map, &serialization_settings).size();
                else
                    sum_size += key_column->serializeValueIntoArena(row, pool, begin, &serialization_settings).size();
            }
            for (const auto & [key_column, null_map, _] : key_columns.other_key_columns)
            {
                if constexpr (nullable)
                    sum_size += key_column->serializeValueIntoArenaWithNull(row, pool, begin, null_map, &serialization_settings).size();
                else
                    sum_size += key_column->serializeValueIntoArena(row, pool, begin, &serialization_settings).size();
            }

            return SerializedKeyHolder{{begin, sum_size}, pool};
        }
    }

    static std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> & key_columns_, const Sizes & sizes, bool optimize)
    {
        ColumnRawPtrs key_columns_copy{key_columns_.begin(), key_columns_.end()};
        Columns new_columns(key_columns_copy, optimize);

        std::vector<IColumn *> new_key_columns;
        new_key_columns.reserve(key_columns_.size());
        Sizes new_key_sizes;
        for (const auto & [key_column, null_map, original_pos] : new_columns.string_key_columns)
        {
            new_key_columns.push_back(key_columns_[original_pos]);
            new_key_sizes.push_back(sizes[original_pos]);
        }
        for (const auto & [key_column, null_map, original_pos] : new_columns.fixed_size_key_columns)
        {
            new_key_columns.push_back(key_columns_[original_pos]);
            new_key_sizes.push_back(sizes[original_pos]);
        }
        for (const auto & [key_column, null_map, original_pos] : new_columns.other_key_columns)
        {
            new_key_columns.push_back(key_columns_[original_pos]);
            new_key_sizes.push_back(sizes[original_pos]);
        }
        key_columns_ = std::move(new_key_columns);
        return new_key_sizes;
    }
};

template <typename Value, typename Mapped, bool nullable, bool prealloc>
struct HashMethodSerializedOld
    : public columns_hashing_impl::HashMethodBase<HashMethodSerializedOld<Value, Mapped, nullable, prealloc>, Value, Mapped, false>
{
    using Self = HashMethodSerializedOld<Value, Mapped, nullable, prealloc>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, false>;

    static HashMethodContextPtr createContext(const HashMethodContextSettings & settings)
    {
        return std::make_shared<HashMethodSerializedContext>(settings);
    }

    static constexpr bool has_cheap_key_calculation = false;

    ColumnRawPtrs key_columns;
    size_t keys_size;
    std::vector<const UInt8 *> null_maps;

    /// Only used if prealloc is true.
    PaddedPODArray<UInt64> row_sizes;
    size_t total_size = 0;
    bool use_batch_serialize = false;
    IColumn::SerializationSettings serialization_settings;
    PaddedPODArray<char> serialized_buffer;
    std::vector<std::string_view> serialized_keys;

    HashMethodSerializedOld(const ColumnRawPtrs & key_columns_, const Sizes & /*key_sizes*/, const HashMethodContextPtr & context, bool)
        : key_columns(key_columns_)
        , keys_size(key_columns_.size())
    {
        const auto * hash_serialized_context = typeid_cast<const HashMethodSerializedContext *>(context.get());
        if (!hash_serialized_context)
        {
            const auto & cached_val = *context;
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Invalid type for HashMethodSerialized context: {}", demangle(typeid(cached_val).name()));
        }

        serialization_settings.serialize_string_with_zero_byte = hash_serialized_context->settings.serialize_string_with_zero_byte;
        if constexpr (nullable)
        {
            null_maps.resize(keys_size, nullptr);
            for (size_t i = 0; i < keys_size; ++i)
            {
                if (const auto * nullable_column = typeid_cast<const ColumnNullable *>(key_columns[i]))
                {
                    null_maps[i] = nullable_column->getNullMapData().data();
                    key_columns[i] = nullable_column->getNestedColumnPtr().get();
                }
            }
        }

        if constexpr (prealloc)
        {
            null_maps.resize(keys_size, nullptr);

            /// Calculate serialized value size for each key column in each row.
            for (size_t i = 0; i < keys_size; ++i)
                key_columns[i]->collectSerializedValueSizes(row_sizes, null_maps[i], &serialization_settings);

            for (auto row_size : row_sizes)
                total_size += row_size;

            use_batch_serialize = shouldUseBatchSerialize();
            if (use_batch_serialize)
            {
                serialized_buffer.resize(total_size);

                const size_t rows = row_sizes.size();
                char * memory = serialized_buffer.data();
                std::vector<char *> memories(rows);
                serialized_keys.resize(rows);
                for (size_t i = 0; i < row_sizes.size(); ++i)
                {
                    memories[i] = memory;
                    serialized_keys[i] = std::string_view(memory, row_sizes[i]);

                    memory += row_sizes[i];
                }

                for (size_t i = 0; i < keys_size; ++i)
                {
                    if constexpr (nullable)
                        key_columns[i]->batchSerializeValueIntoMemoryWithNull(memories, null_maps[i], &serialization_settings);
                    else
                        key_columns[i]->batchSerializeValueIntoMemory(memories, &serialization_settings);
                }
            }
        }
    }

    bool shouldUseBatchSerialize() const
    {
#if defined(__aarch64__)
        // On ARM64 architectures, always use batch serialization, otherwise it would cause performance degradation in related perf tests.
        return true;
#endif

        size_t l2_size = 256 * 1024;
#if defined(OS_LINUX) && defined(_SC_LEVEL2_CACHE_SIZE)
        if (auto ret = sysconf(_SC_LEVEL2_CACHE_SIZE); ret != -1)
            l2_size = ret;
#endif
        // Calculate the average row size.
        size_t avg_row_size = total_size / std::max(row_sizes.size(), 1UL);
        // Use batch serialization only if total size fits in 4x L2 cache and average row size is small.
        return total_size <= 4 * l2_size && avg_row_size < 128;
    }

    friend class columns_hashing_impl::HashMethodBase<Self, Value, Mapped, false>;

    ALWAYS_INLINE ArenaKeyHolder getKeyHolder(size_t row, Arena & pool) const
    requires(prealloc)
    {
        if (use_batch_serialize)
            return ArenaKeyHolder{serialized_keys[row], pool};
        else
        {
            std::unique_ptr<char[]> holder = std::make_unique<char[]>(row_sizes[row]);
            char * memory = holder.get();
            std::string_view key(memory, row_sizes[row]);
            for (size_t j = 0; j < keys_size; ++j)
            {
                if constexpr (nullable)
                    memory = key_columns[j]->serializeValueIntoMemoryWithNull(row, memory, null_maps[j], &serialization_settings);
                else
                    memory = key_columns[j]->serializeValueIntoMemory(row, memory, &serialization_settings);
            }

            return ArenaKeyHolder{key, pool, std::move(holder)};
        }
    }

    ALWAYS_INLINE SerializedKeyHolder getKeyHolder(size_t row, Arena & pool) const
    requires(!prealloc)
    {
        if constexpr (nullable)
        {
            const char * begin = nullptr;

            size_t sum_size = 0;
            for (size_t j = 0; j < keys_size; ++j)
                sum_size += key_columns[j]->serializeValueIntoArenaWithNull(row, pool, begin, null_maps[j], &serialization_settings).size();

            return SerializedKeyHolder{{begin, sum_size}, pool};
        }

        return SerializedKeyHolder{serializeKeysToPoolContiguous(row, keys_size, key_columns, pool, &serialization_settings), pool};
    }
};

template <template <typename Value, typename Mapped, bool nullable, bool prealloc> typename HashMethod, size_t Rows>
void BM_serializedMethod([[maybe_unused]] benchmark::State & state)
{
    MutableColumns cols(2);
    cols[0] = ColumnString::create();
    cols[1] = ColumnString::create();

    pcg64 generator(42);
    std::uniform_int_distribution<size_t> dist;

    for (size_t i = 0; i < Rows; ++i)
    {
        cols[0]->insert("key" + std::to_string(i));
        cols[1]->insert("value" + std::to_string(dist(generator)));
    }

    // Prepare key columns
    ColumnRawPtrs key_columns;
    for (const auto & col : cols)
        key_columns.push_back(col.get());

    std::vector<size_t> indexes(Rows);
    std::iota(indexes.begin(), indexes.end(), 0);
    std::shuffle(indexes.begin(), indexes.end(), generator);

    for (auto _ : state)
    {
        // if (state.iterations() % 10 == 0)
        //     std::shuffle(indexes.begin(), indexes.end(), generator);

        // Prepare hashing method
        ColumnsHashing::HashMethodContext::Settings cache_settings;
        cache_settings.max_threads = 2;
        cache_settings.serialize_string_with_zero_byte = true;
        auto context = HashMethod<UInt128, void, false, false>::createContext(cache_settings);
        using HashTable = TwoLevelHashMapWithSavedHash<std::string_view, AggregateDataPtr>;
        using Value = HashTable::value_type;
        using Mapped = HashTable::mapped_type;

        HashTable table(Rows);
        HashMethod<Value, Mapped, false, true> method(
            key_columns,
            {/*key_sizes*/},
            context,
            /*optimize_=*/true);

        Arena pool;
        for (size_t row = 0; row < Rows; ++row)
        {
            auto holder = method.getKeyHolder(row, pool);
            benchmark::DoNotOptimize(holder.key.data());
            bool inserted = false;
            typename HashTable::LookupResult it;
            table.emplace(holder, it, inserted);
            benchmark::DoNotOptimize(inserted);
        }
    }
}

// int main()
// {
//     BM_serializedMethod<HashMethodSerializedOld>();
//     BM_serializedMethod<HashMethodSerializedNew>();
//     return 0;
// }

BENCHMARK_TEMPLATE(BM_serializedMethod, HashMethodSerializedOld, 32)->Name("HashMethodSerializedOld/5");
BENCHMARK_TEMPLATE(BM_serializedMethod, HashMethodSerializedOld, 64)->Name("HashMethodSerializedOld/6");
BENCHMARK_TEMPLATE(BM_serializedMethod, HashMethodSerializedOld, 128)->Name("HashMethodSerializedOld/7");
BENCHMARK_TEMPLATE(BM_serializedMethod, HashMethodSerializedOld, 256)->Name("HashMethodSerializedOld/8");
BENCHMARK_TEMPLATE(BM_serializedMethod, HashMethodSerializedOld, 512)->Name("HashMethodSerializedOld/9");
BENCHMARK_TEMPLATE(BM_serializedMethod, HashMethodSerializedOld, 1024)->Name("HashMethodSerializedOld/10");
BENCHMARK_TEMPLATE(BM_serializedMethod, HashMethodSerializedOld, 2048)->Name("HashMethodSerializedOld/11");
BENCHMARK_TEMPLATE(BM_serializedMethod, HashMethodSerializedOld, 4096)->Name("HashMethodSerializedOld/12");
BENCHMARK_TEMPLATE(BM_serializedMethod, HashMethodSerializedOld, 8192)->Name("HashMethodSerializedOld/13");
BENCHMARK_TEMPLATE(BM_serializedMethod, HashMethodSerializedOld, 16384)->Name("HashMethodSerializedOld/14");
BENCHMARK_TEMPLATE(BM_serializedMethod, HashMethodSerializedOld, 32768)->Name("HashMethodSerializedOld/15");
BENCHMARK_TEMPLATE(BM_serializedMethod, HashMethodSerializedOld, 65536)->Name("HashMethodSerializedOld/16");
BENCHMARK_TEMPLATE(BM_serializedMethod, HashMethodSerializedOld, 131072)->Name("HashMethodSerializedOld/17");
BENCHMARK_TEMPLATE(BM_serializedMethod, HashMethodSerializedOld, 262144)->Name("HashMethodSerializedOld/18");

BENCHMARK_TEMPLATE(BM_serializedMethod, HashMethodSerializedNew, 32)->Name("HashMethodSerializedNew/5");
BENCHMARK_TEMPLATE(BM_serializedMethod, HashMethodSerializedNew, 64)->Name("HashMethodSerializedNew/6");
BENCHMARK_TEMPLATE(BM_serializedMethod, HashMethodSerializedNew, 128)->Name("HashMethodSerializedNew/7");
BENCHMARK_TEMPLATE(BM_serializedMethod, HashMethodSerializedNew, 256)->Name("HashMethodSerializedNew/8");
BENCHMARK_TEMPLATE(BM_serializedMethod, HashMethodSerializedNew, 512)->Name("HashMethodSerializedNew/9");
BENCHMARK_TEMPLATE(BM_serializedMethod, HashMethodSerializedNew, 1024)->Name("HashMethodSerializedNew/10");
BENCHMARK_TEMPLATE(BM_serializedMethod, HashMethodSerializedNew, 2048)->Name("HashMethodSerializedNew/11");
BENCHMARK_TEMPLATE(BM_serializedMethod, HashMethodSerializedNew, 4096)->Name("HashMethodSerializedNew/12");
BENCHMARK_TEMPLATE(BM_serializedMethod, HashMethodSerializedNew, 8192)->Name("HashMethodSerializedNew/13");
BENCHMARK_TEMPLATE(BM_serializedMethod, HashMethodSerializedNew, 16384)->Name("HashMethodSerializedNew/14");
BENCHMARK_TEMPLATE(BM_serializedMethod, HashMethodSerializedNew, 32768)->Name("HashMethodSerializedNew/15");
BENCHMARK_TEMPLATE(BM_serializedMethod, HashMethodSerializedNew, 65536)->Name("HashMethodSerializedNew/16");
BENCHMARK_TEMPLATE(BM_serializedMethod, HashMethodSerializedNew, 131072)->Name("HashMethodSerializedNew/17");
BENCHMARK_TEMPLATE(BM_serializedMethod, HashMethodSerializedNew, 262144)->Name("HashMethodSerializedNew/18");
