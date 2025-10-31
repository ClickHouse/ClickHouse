#include <Columns/ColumnCompressed.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnReplicated.h>
#include <Common/WeakHash.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
}

std::atomic<UInt64> ColumnReplicated::global_id_counter = 0;

ColumnReplicated::ColumnReplicated(MutableColumnPtr && nested_column_)
    : nested_column(std::move(nested_column_)), id(global_id_counter.fetch_add(1))
{
    indexes.insertIndexesRange(0, nested_column->size());
}

ColumnReplicated::ColumnReplicated(MutableColumnPtr && nested_column_, MutableColumnPtr && indexes_)
    : nested_column(std::move(nested_column_))
    , indexes(std::move(indexes_))
    , id(global_id_counter.fetch_add(1))
{
}

ColumnReplicated::ColumnReplicated(MutableColumnPtr && nested_column_, ColumnIndex && indexes_)
    : nested_column(std::move(nested_column_))
    , indexes(std::move(indexes_))
    , id(global_id_counter.fetch_add(1))
{
}

MutableColumnPtr ColumnReplicated::cloneResized(size_t new_size) const
{
    if (new_size == 0)
        return cloneEmpty();

    if (new_size == size())
        return create(mutate(nested_column), mutate(indexes.getIndexes()));

    /// If new size is larger than current size, we fill all new rows with default value of nested column.
    if (new_size > size())
    {
        auto new_nested_column = mutate(nested_column);
        new_nested_column->insertDefault();
        auto new_indexes = ColumnIndex(mutate(indexes.getIndexes()));
        new_indexes.insertManyIndexes(new_nested_column->size() - 1, new_size - size());
        return create(std::move(new_nested_column), std::move(new_indexes));
    }

    auto res = create(nested_column->cloneEmpty());
    res->insertRangeFrom(*this, 0, new_size);
    return res;
}

MutableColumnPtr ColumnReplicated::cloneEmpty() const
{
    return create(nested_column->cloneEmpty(), indexes.getIndexes()->cloneEmpty());
}

bool ColumnReplicated::isDefaultAt(size_t n) const
{
    return nested_column->isDefaultAt(indexes.getIndexAt(n));
}

bool ColumnReplicated::isNullAt(size_t n) const
{
    return nested_column->isNullAt(indexes.getIndexAt(n));
}

Field ColumnReplicated::operator[](size_t n) const
{
    return (*nested_column)[indexes.getIndexAt(n)];
}

void ColumnReplicated::get(size_t n, Field & res) const
{
    nested_column->get(indexes.getIndexAt(n), res);
}

DataTypePtr ColumnReplicated::getValueNameAndTypeImpl(WriteBufferFromOwnString & name_buf, size_t n, const IColumn::Options & options) const
{
    return nested_column->getValueNameAndTypeImpl(name_buf, indexes.getIndexAt(n), options);
}

bool ColumnReplicated::getBool(size_t n) const
{
    return nested_column->getBool(indexes.getIndexAt(n));
}

Float64 ColumnReplicated::getFloat64(size_t n) const
{
    return nested_column->getFloat64(indexes.getIndexAt(n));
}

Float32 ColumnReplicated::getFloat32(size_t n) const
{
    return nested_column->getFloat32(indexes.getIndexAt(n));
}

UInt64 ColumnReplicated::getUInt(size_t n) const
{
    return nested_column->getUInt(indexes.getIndexAt(n));
}

Int64 ColumnReplicated::getInt(size_t n) const
{
    return nested_column->getInt(indexes.getIndexAt(n));
}

UInt64 ColumnReplicated::get64(size_t n) const
{
    return nested_column->get64(indexes.getIndexAt(n));
}

StringRef ColumnReplicated::getDataAt(size_t n) const
{
    return nested_column->getDataAt(indexes.getIndexAt(n));
}

ColumnPtr ColumnReplicated::convertToFullColumnIfReplicated() const
{
    return nested_column->index(*indexes.getIndexes(), 0);
}

void ColumnReplicated::insertData(const char * pos, size_t length)
{
    nested_column->insertData(pos, length);
    indexes.insertIndex(nested_column->size() - 1);
}

StringRef ColumnReplicated::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
    return nested_column->serializeValueIntoArena(indexes.getIndexAt(n), arena, begin);
}

StringRef ColumnReplicated::serializeAggregationStateValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
    return nested_column->serializeAggregationStateValueIntoArena(indexes.getIndexAt(n), arena, begin);
}

char * ColumnReplicated::serializeValueIntoMemory(size_t n, char * memory) const
{
    return nested_column->serializeValueIntoMemory(indexes.getIndexAt(n), memory);
}

std::optional<size_t> ColumnReplicated::getSerializedValueSize(size_t n) const
{
    return nested_column->getSerializedValueSize(indexes.getIndexAt(n));
}

const char * ColumnReplicated::deserializeAndInsertFromArena(const char * pos)
{
    const auto * res = nested_column->deserializeAndInsertFromArena(pos);
    indexes.insertIndex(nested_column->size() - 1);
    return res;
}

const char * ColumnReplicated::deserializeAndInsertAggregationStateValueFromArena(const char * pos)
{
    const auto * res = nested_column->deserializeAndInsertAggregationStateValueFromArena(pos);
    indexes.insertIndex(nested_column->size() - 1);
    return res;
}

const char * ColumnReplicated::skipSerializedInArena(const char * pos) const
{
    return nested_column->skipSerializedInArena(pos);
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnReplicated::insertRangeFrom(const IColumn & src, size_t start, size_t length)
#else
void ColumnReplicated::doInsertRangeFrom(const IColumn & src, size_t start, size_t length)
#endif
{
    if (length == 0)
        return;

    if (start + length > src.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Parameter out of bound in ColumnReplicated::insertRangeFrom method.");

    if (const auto * src_replicated = typeid_cast<const ColumnReplicated *>(&src))
    {
        /// Optimization for case when we insert the whole column (may happen in squashing).
        if (start == 0 && length == src_replicated->size())
        {
            indexes.insertIndexesRangeWithShift(*src_replicated->getIndexesColumn(), start, length, nested_column->size(), nested_column->size() + src_replicated->getNestedColumn()->size());
            nested_column->insertRangeFrom(*src_replicated->getNestedColumn(), 0, src_replicated->getNestedColumn()->size());
        }
        else
        {
            /// Use insertion_cache to avoid copying of values from source nested column if
            /// we already inserted them earlier and can use indexes of already inserted values.
            auto & indexes_match = insertion_cache[src_replicated->id];

            auto insert = [&](size_t, size_t src_index)
            {
                auto it = indexes_match.find(src_index);
                if (it == indexes_match.end())
                {
                    nested_column->insertFrom(*src_replicated->nested_column, src_index);
                    it = indexes_match.emplace(src_index, nested_column->size() - 1).first;
                }

                indexes.insertIndex(it->second);
            };

            src_replicated->indexes.callForIndexes(std::move(insert), start, start + length);
        }
    }
    else
    {
        size_t old_size = nested_column->size();
        nested_column->insertRangeFrom(src, start, length);
        indexes.insertIndexesRange(old_size, length);
    }
}

void ColumnReplicated::insert(const Field & x)
{
    nested_column->insert(x);
    indexes.insertIndex(nested_column->size() - 1);
}

bool ColumnReplicated::tryInsert(const Field & x)
{
    if (nested_column->tryInsert(x))
    {
        indexes.insertIndex(nested_column->size() - 1);
        return true;
    }

    return false;
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnReplicated::insertFrom(const IColumn & src, size_t n)
#else
void ColumnReplicated::doInsertFrom(const IColumn & src, size_t n)
#endif
{
    if (const auto * src_replicated = typeid_cast<const ColumnReplicated *>(&src))
    {
        /// Use insertion_cache to avoid copying of values from source nested column if
        /// we already inserted them earlier and can use indexes of already inserted values.
        auto & indexes_match = insertion_cache[src_replicated->id];
        auto src_index = src_replicated->indexes.getIndexAt(n);
        auto it = indexes_match.find(src_index);
        if (it == indexes_match.end())
        {
            nested_column->insertFrom(*src_replicated->nested_column, src_index);
            it = indexes_match.emplace(src_index, nested_column->size() - 1).first;
        }

        indexes.insertIndex(it->second);
    }
    else
    {
        nested_column->insertFrom(src, n);
        indexes.insertIndex(nested_column->size() - 1);
    }
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnReplicated::insertManyFrom(const IColumn & src, size_t n, size_t length)
#else
void ColumnReplicated::doInsertManyFrom(const IColumn & src, size_t n, size_t length)
#endif
{
    if (const auto * src_replicated = typeid_cast<const ColumnReplicated *>(&src))
    {
        /// Use insertion_cache to avoid copying of values from source nested column if
        /// we already inserted them earlier and can use indexes of already inserted values.
        auto & indexes_match = insertion_cache[src_replicated->id];
        auto src_index = src_replicated->indexes.getIndexAt(n);
        auto it = indexes_match.find(src_index);
        if (it == indexes_match.end())
        {
            nested_column->insertFrom(*src_replicated->nested_column, src_index);
            it = indexes_match.emplace(src_index, nested_column->size() - 1).first;
        }

        indexes.insertManyIndexes(it->second, length);
    }
    else
    {
        nested_column->insertFrom(src, n);
        indexes.insertManyIndexes(nested_column->size() - 1, length);
    }
}

void ColumnReplicated::insertDefault()
{
    nested_column->insertDefault();
    indexes.insertIndex(nested_column->size() - 1);
}

void ColumnReplicated::insertManyDefaults(size_t length)
{
    nested_column->insertDefault();
    indexes.insertManyIndexes(nested_column->size() - 1, length);
}

void ColumnReplicated::popBack(size_t n)
{
    indexes.popBack(n);
    nested_column = indexes.removeUnusedRowsInIndexedData(std::move(nested_column));
}

ColumnPtr ColumnReplicated::filter(const Filter & filt, ssize_t result_size_hint) const
{
    if (size() != filt.size())
        throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Size of filter ({}) doesn't match size of column ({})", filt.size(), size());

    auto filtered_indexes = ColumnIndex(indexes.getIndexes()->filter(filt, result_size_hint));
    auto filtered_nested_column = filtered_indexes.removeUnusedRowsInIndexedData(nested_column);
    return create(filtered_nested_column, std::move(filtered_indexes));
}

void ColumnReplicated::expand(const Filter & mask, bool inverted)
{
    indexes.expand(mask, inverted);
}

ColumnPtr ColumnReplicated::permute(const Permutation & perm, size_t limit) const
{
    if (size() != perm.size())
        throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Size of permutation ({}) doesn't match size of column ({})", perm.size(), size());

    auto permuted_indexes = ColumnIndex(indexes.getIndexes()->permute(perm, limit));
    auto filtered_nested_column = permuted_indexes.removeUnusedRowsInIndexedData(nested_column);
    return create(filtered_nested_column, std::move(permuted_indexes));
}

ColumnPtr ColumnReplicated::index(const IColumn & res_indexes, size_t limit) const
{
    auto indexed_indexes = ColumnIndex(indexes.getIndexes()->index(res_indexes, limit));
    auto filtered_nested_column = indexed_indexes.removeUnusedRowsInIndexedData(nested_column);
    return create(filtered_nested_column, std::move(indexed_indexes));
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
int ColumnReplicated::compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const
#else
int ColumnReplicated::doCompareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const
#endif
{
    if (const auto * rhs_replicated = typeid_cast<const ColumnReplicated *>(&rhs))
        return nested_column->compareAt(indexes.getIndexAt(n), rhs_replicated->indexes.getIndexAt(m), *rhs_replicated->nested_column, nan_direction_hint);

    return nested_column->compareAt(indexes.getIndexAt(n), m, rhs, nan_direction_hint);
}

int ColumnReplicated::compareAtWithCollation(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint, const Collator & collator) const
{
    if (const auto * rhs_replicated = typeid_cast<const ColumnReplicated *>(&rhs))
        return nested_column->compareAtWithCollation(indexes.getIndexAt(n), rhs_replicated->indexes.getIndexAt(m), *rhs_replicated->nested_column, nan_direction_hint, collator);

    return nested_column->compareAtWithCollation(indexes.getIndexAt(n), m, rhs, nan_direction_hint, collator);
}

bool ColumnReplicated::hasEqualValues() const
{
    return indexes.getIndexes()->hasEqualValues();
}

struct ColumnReplicated::ComparatorBase
{
    const ColumnReplicated & parent;
    int nan_direction_hint;

    ComparatorBase(const ColumnReplicated & parent_, int nan_direction_hint_)
        : parent(parent_), nan_direction_hint(nan_direction_hint_)
    {
    }

    ALWAYS_INLINE int compare(size_t lhs, size_t rhs) const
    {
        int res = parent.compareAt(lhs, rhs, parent, nan_direction_hint);

        return res;
    }
};

struct ColumnReplicated::ComparatorCollationBase
{
    const ColumnReplicated & parent;
    int nan_direction_hint;
    const Collator * collator;

    ComparatorCollationBase(const ColumnReplicated & parent_, int nan_direction_hint_, const Collator * collator_)
        : parent(parent_), nan_direction_hint(nan_direction_hint_), collator(collator_)
    {
    }

    ALWAYS_INLINE int compare(size_t lhs, size_t rhs) const
    {
        int res = parent.compareAtWithCollation(lhs, rhs, parent, nan_direction_hint, *collator);

        return res;
    }
};

void ColumnReplicated::getPermutation(PermutationSortDirection direction, PermutationSortStability stability,
                                size_t limit, int nan_direction_hint, Permutation & res) const
{
    if (direction == PermutationSortDirection::Ascending && stability == PermutationSortStability::Unstable)
        getPermutationImpl(limit, res, ComparatorAscendingUnstable(*this, nan_direction_hint), DefaultSort(), DefaultPartialSort());
    else if (direction == PermutationSortDirection::Ascending && stability == PermutationSortStability::Stable)
        getPermutationImpl(limit, res, ComparatorAscendingStable(*this, nan_direction_hint), DefaultSort(), DefaultPartialSort());
    else if (direction == PermutationSortDirection::Descending && stability == PermutationSortStability::Unstable)
        getPermutationImpl(limit, res, ComparatorDescendingUnstable(*this, nan_direction_hint), DefaultSort(), DefaultPartialSort());
    else if (direction == PermutationSortDirection::Descending && stability == PermutationSortStability::Stable)
        getPermutationImpl(limit, res, ComparatorDescendingStable(*this, nan_direction_hint), DefaultSort(), DefaultPartialSort());
}

void ColumnReplicated::updatePermutation(PermutationSortDirection direction, PermutationSortStability stability,
                                size_t limit, int nan_direction_hint, Permutation & res, EqualRanges & equal_ranges) const
{
    auto comparator_equal = ComparatorEqual(*this, nan_direction_hint);

    if (direction == PermutationSortDirection::Ascending && stability == PermutationSortStability::Unstable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorAscendingUnstable(*this, nan_direction_hint), comparator_equal, DefaultSort(), DefaultPartialSort());
    else if (direction == PermutationSortDirection::Ascending && stability == PermutationSortStability::Stable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorAscendingStable(*this, nan_direction_hint), comparator_equal, DefaultSort(), DefaultPartialSort());
    else if (direction == PermutationSortDirection::Descending && stability == PermutationSortStability::Unstable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorDescendingUnstable(*this, nan_direction_hint), comparator_equal, DefaultSort(), DefaultPartialSort());
    else if (direction == PermutationSortDirection::Descending && stability == PermutationSortStability::Stable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorDescendingStable(*this, nan_direction_hint), comparator_equal, DefaultSort(), DefaultPartialSort());
}

void ColumnReplicated::getPermutationWithCollation(const Collator & collator, PermutationSortDirection direction, PermutationSortStability stability,
                                            size_t limit, int nan_direction_hint, Permutation & res) const
{
    if (direction == PermutationSortDirection::Ascending && stability == PermutationSortStability::Unstable)
        getPermutationImpl(limit, res, ComparatorCollationAscendingUnstable(*this, nan_direction_hint, &collator), DefaultSort(), DefaultPartialSort());
    else if (direction == PermutationSortDirection::Ascending && stability == PermutationSortStability::Stable)
        getPermutationImpl(limit, res, ComparatorCollationAscendingStable(*this, nan_direction_hint, &collator), DefaultSort(), DefaultPartialSort());
    else if (direction == PermutationSortDirection::Descending && stability == PermutationSortStability::Unstable)
        getPermutationImpl(limit, res, ComparatorCollationDescendingUnstable(*this, nan_direction_hint, &collator), DefaultSort(), DefaultPartialSort());
    else if (direction == PermutationSortDirection::Descending && stability == PermutationSortStability::Stable)
        getPermutationImpl(limit, res, ComparatorCollationDescendingStable(*this, nan_direction_hint, &collator), DefaultSort(), DefaultPartialSort());
}

void ColumnReplicated::updatePermutationWithCollation(const Collator & collator, PermutationSortDirection direction, PermutationSortStability stability,
                                            size_t limit, int nan_direction_hint, Permutation & res, EqualRanges & equal_ranges) const
{
    auto comparator_equal = ComparatorCollationEqual(*this, nan_direction_hint, &collator);

    if (direction == PermutationSortDirection::Ascending && stability == PermutationSortStability::Unstable)
        updatePermutationImpl(
            limit,
            res,
            equal_ranges,
            ComparatorCollationAscendingUnstable(*this, nan_direction_hint, &collator),
            comparator_equal,
            DefaultSort(),
            DefaultPartialSort());
    else if (direction == PermutationSortDirection::Ascending && stability == PermutationSortStability::Stable)
        updatePermutationImpl(
            limit,
            res,
            equal_ranges,
            ComparatorCollationAscendingStable(*this, nan_direction_hint, &collator),
            comparator_equal,
            DefaultSort(),
            DefaultPartialSort());
    else if (direction == PermutationSortDirection::Descending && stability == PermutationSortStability::Unstable)
        updatePermutationImpl(
            limit,
            res,
            equal_ranges,
            ComparatorCollationDescendingUnstable(*this, nan_direction_hint, &collator),
            comparator_equal,
            DefaultSort(),
            DefaultPartialSort());
    else if (direction == PermutationSortDirection::Descending && stability == PermutationSortStability::Stable)
        updatePermutationImpl(
            limit,
            res,
            equal_ranges,
            ComparatorCollationDescendingStable(*this, nan_direction_hint, &collator),
            comparator_equal,
            DefaultSort(),
            DefaultPartialSort());
}

size_t ColumnReplicated::byteSize() const
{
    return indexes.getIndexes()->byteSize() + nested_column->byteSize();
}

size_t ColumnReplicated::byteSizeAt(size_t n) const
{
    return nested_column->byteSizeAt(indexes.getIndexAt(n));
}

size_t ColumnReplicated::allocatedBytes() const
{
    return indexes.getIndexes()->allocatedBytes() + nested_column->allocatedBytes();
}

void ColumnReplicated::protect()
{
    indexes.getIndexesPtr()->protect();
    nested_column->protect();
}

ColumnPtr ColumnReplicated::replicate(const Offsets & offsets) const
{
    auto replicated_indexes = ColumnIndex(indexes.getIndexes()->replicate(offsets));
    auto filtered_nested_column = replicated_indexes.removeUnusedRowsInIndexedData(nested_column);
    return create(filtered_nested_column, std::move(replicated_indexes));
}

void ColumnReplicated::updateHashWithValue(size_t n, SipHash & hash) const
{
    nested_column->updateHashWithValue(indexes.getIndexAt(n), hash);
}

WeakHash32 ColumnReplicated::getWeakHash32() const
{
    WeakHash32 nested_column_hash = nested_column->getWeakHash32();
    return indexes.getWeakHash(nested_column_hash);
}

void ColumnReplicated::updateHashFast(SipHash & hash) const
{
    indexes.getIndexes()->updateHashFast(hash);
    nested_column->updateHashFast(hash);
}

void ColumnReplicated::getExtremes(Field & min, Field & max) const
{
    /// It might happen that some indexes are unused, so we cannot call nested_column->getExtremes.
    nested_column->index(*indexes.getIndexes(), 0)->getExtremes(min, max);
}

void ColumnReplicated::getIndicesOfNonDefaultRows(Offsets & result_indexes, size_t from, size_t limit) const
{
    PaddedPODArray<UInt8> default_values_mask(nested_column->size());
    for (size_t i = 0; i != nested_column->size(); ++i)
        default_values_mask[i] = !nested_column->isDefaultAt(i);

    size_t to = limit && from + limit < size() ? from + limit : size();
    indexes.getIndexesByMask(result_indexes, default_values_mask, from, to);
}

UInt64 ColumnReplicated::getNumberOfDefaultRows() const
{
    std::unordered_set<size_t> indexes_of_default_values;
    for (size_t i = 0; i != nested_column->size(); ++i)
    {
        if (nested_column->isDefaultAt(i))
            indexes_of_default_values.insert(i);
    }

    size_t result = 0;
    auto add = [&](size_t, size_t index)
    {
        result += indexes_of_default_values.contains(index);
    };

    indexes.callForIndexes(std::move(add), 0, size());
    return result;
}

ColumnPtr ColumnReplicated::compress(bool force_compression) const
{
    auto nested_column_compressed = nested_column->compress(force_compression);
    auto indexes_compressed = indexes.getIndexes()->compress(force_compression);

    size_t byte_size = nested_column_compressed->byteSize() + indexes_compressed->byteSize();

    return ColumnCompressed::create(size(), byte_size,
        [my_nested_column_compressed = std::move(nested_column_compressed), my_indexes_compressed = std::move(indexes_compressed)]
        {
            return ColumnReplicated::create(my_nested_column_compressed->decompress(), my_indexes_compressed->decompress());
        });
}

ColumnCheckpointPtr ColumnReplicated::getCheckpoint() const
{
    return std::make_shared<ColumnCheckpointWithNested>(size(), nested_column->getCheckpoint());
}

void ColumnReplicated::updateCheckpoint(ColumnCheckpoint & checkpoint) const
{
    checkpoint.size = size();
    nested_column->updateCheckpoint(*assert_cast<ColumnCheckpointWithNested &>(checkpoint).nested);
}

void ColumnReplicated::rollback(const ColumnCheckpoint & checkpoint)
{
    const auto & nested = *assert_cast<const ColumnCheckpointWithNested &>(checkpoint).nested;

    nested_column->rollback(nested);
    indexes.resizeAssumeReserve(nested.size);
}

void ColumnReplicated::forEachMutableSubcolumn(MutableColumnCallback callback)
{
    callback(nested_column);
    callback(indexes.getIndexesPtr());
}

void ColumnReplicated::forEachMutableSubcolumnRecursively(RecursiveMutableColumnCallback callback)
{
    callback(*nested_column);
    nested_column->forEachMutableSubcolumnRecursively(callback);
    callback(*indexes.getIndexesPtr());
    indexes.getIndexesPtr()->forEachMutableSubcolumnRecursively(callback);
}

void ColumnReplicated::forEachSubcolumn(ColumnCallback callback) const
{
    callback(nested_column);
    callback(indexes.getIndexes());
}

void ColumnReplicated::forEachSubcolumnRecursively(RecursiveColumnCallback callback) const
{
    callback(*nested_column);
    nested_column->forEachSubcolumnRecursively(callback);
    callback(*indexes.getIndexes());
    indexes.getIndexes()->forEachSubcolumnRecursively(callback);
}

bool ColumnReplicated::structureEquals(const IColumn & rhs) const
{
    if (const auto * rhs_replicated = typeid_cast<const ColumnReplicated *>(&rhs))
        return nested_column->structureEquals(*rhs_replicated->nested_column);
    return false;
}

void ColumnReplicated::takeDynamicStructureFromSourceColumns(const Columns & source_columns, std::optional<size_t> max_dynamic_subcolumns)
{
    Columns source_nested_columns;
    source_nested_columns.reserve(source_columns.size());
    for (const auto & source_column : source_columns)
    {
        if (const auto * rhs_replicated = typeid_cast<const ColumnReplicated *>(source_column.get()))
            source_nested_columns.emplace_back(rhs_replicated->nested_column);
        else
            source_nested_columns.emplace_back(source_column);
    }

    nested_column->takeDynamicStructureFromSourceColumns(source_nested_columns, max_dynamic_subcolumns);
}

void ColumnReplicated::takeDynamicStructureFromColumn(const ColumnPtr & source_column)
{
    if (const auto * rhs_replicated = typeid_cast<const ColumnReplicated *>(source_column.get()))
        nested_column->takeDynamicStructureFromColumn(rhs_replicated->nested_column);
    else
        nested_column->takeDynamicStructureFromColumn(source_column);
}

namespace
{

template <typename T>
ColumnPtr convertOffsetsToIndexesImpl(const IColumn::Offsets & offsets)
{
    auto result = ColumnVector<T>::create();
    auto & data = result->getData();
    data.reserve_exact(offsets.back());
    for (size_t i = 0; i != offsets.size(); ++i)
        data.resize_fill(data.size() + offsets[i] - offsets[i - 1], i);
    return result;
}

}

ColumnPtr convertOffsetsToIndexes(const IColumn::Offsets & offsets)
{
    size_t max_index = offsets.size();
    if (max_index <= std::numeric_limits<UInt8>::max())
        return convertOffsetsToIndexesImpl<UInt8>(offsets);
    if (max_index <= std::numeric_limits<UInt16>::max())
        return convertOffsetsToIndexesImpl<UInt16>(offsets);
    if (max_index <= std::numeric_limits<UInt32>::max())
        return convertOffsetsToIndexesImpl<UInt32>(offsets);
    return convertOffsetsToIndexesImpl<UInt64>(offsets);
}

bool isLazyReplicationUseful(const ColumnPtr & column)
{
    return !column->isConst() && !column->isReplicated() && !column->lowCardinality() && (!column->isFixedAndContiguous() || column->sizeOfValueIfFixed() > 8);
}


}
