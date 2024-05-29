#pragma once

#include <Columns/IColumn.h>
#include <Columns/ColumnVector.h>


namespace DB
{

/**
 * Column for storing Variant(...) type values.
 * Variant type represents a union of other data types.
 * For example, type Variant(T1, T2, ..., TN) means that each row of this type
 * has a value of either type T1 or T2 or ... or TN or none of them (NULL value)
 *
 * ColumnVariant stores:
 *   - The discriminators column, which determines which variant is stored in each row.
 *   - The offsets column, which determines the offset in the corresponding variant column in each row.
 *   - The list of variant columns with only real values (so the sizes of variant columns can be different).
 * Discriminator is an index of a variant in the variants list, it also has special value called NULL_DISCRIMINATOR
 * that indicates that the value in the row is NULL.
 *
 * We want to be able to extend Variant column for free without rewriting the data, but as we don't care about the
 * order of variants during Variant creation (we want Variant(T1, T2) to be the same as Variant(T2, T1)), we support
 * some global order of nested types inside Variant during type creation, so after extension the order of variant types
 * (and so their discriminators) can change. For example: Variant(T1, T3) -> Variant(T1, T2, T3).
 * To avoid full rewrite of discriminators column on Variant extension, we differentiate local order of variants
 * inside a column and global order of variants created during type creation. So, ColumnVariant stores only local
 * discriminators and additionally stores the mapping between global and local discriminators.
 * So, when we need to extend Variant column with new variant, we can just append it to a list of variant columns
 * with new local discriminator and update mapping from global to local orders.
 *
 * Note that two instances of ColumnVariant can have different local orders, so we should always use global
 * discriminators during inter-column interactions.
 *
 * Let's take an example with type Variant(UInt32, String, Array(UInt32)):
 * During type creation we will sort types by their names and get the global order: Array(UInt32), String, UInt32.
 * So, type Array(UInt32) will have global discriminator 0, String - 1 and UInt32 - 2.
 * Let's say we have a column with local order (String, UInt32, Array(UInt32)) and values:
 * 'Hello', 42, NULL, 'World', 43, [1, 2, 3], NULL, 44
 *
 * Let's see how these values will be stored in ColumnVariant:
 *
 * local_to_global_discriminators: {0 : 1, 1 : 2, 2 : 0}
 * global_to_local_discriminators: {0 : 2, 1 : 0, 2 : 1}
 * local_discriminators    offsets    String    UInt32    Array(UInt32)
 *         0                  0       'Hello'     42        [1, 2, 3]
 *         1                  0       'World'     43
 *  NULL_DISCRIMINATOR        0                   44
 *         0                  1
 *         1                  1
 *         2                  0
 *  NULL_DISCRIMINATOR        0
 *         1                  2
 *
 */
class ColumnVariant final : public COWHelper<IColumn, ColumnVariant>
{
public:
    using Discriminator = UInt8;
    using Discriminators = PaddedPODArray<Discriminator>;
    using ColumnDiscriminators = ColumnVector<Discriminator>;
    using ColumnOffsets = ColumnVector<Offset>;

    static constexpr UInt8 NULL_DISCRIMINATOR = std::numeric_limits<Discriminator>::max(); /// 255
    static constexpr size_t MAX_NESTED_COLUMNS = std::numeric_limits<Discriminator>::max(); /// 255

    struct ComparatorBase;

    using ComparatorAscendingUnstable = ComparatorAscendingUnstableImpl<ComparatorBase>;
    using ComparatorAscendingStable = ComparatorAscendingStableImpl<ComparatorBase>;
    using ComparatorDescendingUnstable = ComparatorDescendingUnstableImpl<ComparatorBase>;
    using ComparatorDescendingStable = ComparatorDescendingStableImpl<ComparatorBase>;
    using ComparatorEqual = ComparatorEqualImpl<ComparatorBase>;

private:
    friend class COWHelper<IColumn, ColumnVariant>;

    using NestedColumns = std::vector<WrappedPtr>;

    /// Create an empty column with provided variants.
    /// Variants are in global order.
    explicit ColumnVariant(MutableColumns && variants_);
    /// Variants are in local order according to provided mapping.
    explicit ColumnVariant(MutableColumns && variants_, const std::vector<Discriminator> & local_to_global_discriminators_);

    /// Create column from discriminators column and list of variant columns.
    /// Offsets column should be constructed according to the discriminators.
    /// Variants are in global order.
    ColumnVariant(MutableColumnPtr local_discriminators_, MutableColumns && variants_);
    /// Variants are in local order according to provided mapping.
    ColumnVariant(MutableColumnPtr local_discriminators_, MutableColumns && variants_, const std::vector<Discriminator> & local_to_global_discriminators_);

    /// Create column from discriminators column, offsets column and list of variant columns.
    /// Variants are in global order.
    ColumnVariant(MutableColumnPtr local_discriminators_, MutableColumnPtr offsets_, MutableColumns && variants_);
    /// Variants are in local order according to provided mapping.
    ColumnVariant(MutableColumnPtr local_discriminators_, MutableColumnPtr offsets_, MutableColumns && variants_, const std::vector<Discriminator> & local_to_global_discriminators_);

    ColumnVariant(const ColumnVariant &) = default;

public:
    /** Create immutable column using immutable arguments. This arguments may be shared with other variants.
      * Use IColumn::mutate in order to make mutable column and mutate shared nested variants.
      */
    using Base = COWHelper<IColumn, ColumnVariant>;
    static Ptr create(const Columns & variants_) { return create(variants_, {}); }
    static Ptr create(const Columns & variants_, const std::vector<Discriminator> & local_to_global_discriminators_);
    static Ptr create(const ColumnPtr & local_discriminators_, const Columns & variants_) { return create(local_discriminators_, variants_, {}); }
    static Ptr create(const ColumnPtr & local_discriminators_, const Columns & variants_, const std::vector<Discriminator> & local_to_global_discriminators_);
    static Ptr create(const ColumnPtr & local_discriminators_, const DB::ColumnPtr & offsets_, const Columns & variants_) { return create(local_discriminators_, offsets_, variants_, {}); }
    static Ptr create(const ColumnPtr & local_discriminators_, const DB::ColumnPtr & offsets_, const Columns & variants_, const std::vector<Discriminator> & local_to_global_discriminators_);

    static MutablePtr create(MutableColumns && variants_)
    {
        return Base::create(std::move(variants_));
    }

    static MutablePtr create(MutableColumns && variants_, const std::vector<Discriminator> & local_to_global_discriminators_)
    {
        return Base::create(std::move(variants_), local_to_global_discriminators_);
    }

    static MutablePtr create(MutableColumnPtr local_discriminators_, MutableColumns && variants_)
    {
        return Base::create(std::move(local_discriminators_), std::move(variants_));
    }

    static MutablePtr create(MutableColumnPtr local_discriminators_, MutableColumns && variants_, const std::vector<Discriminator> & local_to_global_discriminators_)
    {
        return Base::create(std::move(local_discriminators_), std::move(variants_), local_to_global_discriminators_);
    }

    static MutablePtr create(MutableColumnPtr local_discriminators_, MutableColumnPtr offsets_, MutableColumns && variants_)
    {
        return Base::create(std::move(local_discriminators_), std::move(offsets_), std::move(variants_));
    }

    static MutablePtr create(MutableColumnPtr local_discriminators_, MutableColumnPtr offsets_, MutableColumns && variants_, const std::vector<Discriminator> & local_to_global_discriminators_)
    {
        return Base::create(std::move(local_discriminators_), std::move(offsets_), std::move(variants_), local_to_global_discriminators_);
    }

    std::string getName() const override;
    const char * getFamilyName() const override { return "Variant"; }
    TypeIndex getDataType() const override { return TypeIndex::Variant; }

    MutableColumnPtr cloneEmpty() const override;
    MutableColumnPtr cloneResized(size_t size) const override;

    size_t ALWAYS_INLINE offsetAt(size_t i) const { return getOffsets()[i]; }
    Discriminator ALWAYS_INLINE localDiscriminatorAt(size_t i) const { return getLocalDiscriminators()[i]; }
    Discriminator ALWAYS_INLINE globalDiscriminatorAt(size_t i) const { return globalDiscriminatorByLocal(getLocalDiscriminators()[i]); }

    Discriminator ALWAYS_INLINE globalDiscriminatorByLocal(Discriminator local_discr) const
    {
        /// NULL_DISCRIMINATOR is always the same in local and global orders.
        return local_discr == NULL_DISCRIMINATOR ? NULL_DISCRIMINATOR : local_to_global_discriminators[local_discr];
    }

    Discriminator ALWAYS_INLINE localDiscriminatorByGlobal(Discriminator global_discr) const
    {
        /// NULL_DISCRIMINATOR is always the same in local and global orders.
        return global_discr == NULL_DISCRIMINATOR ? NULL_DISCRIMINATOR : global_to_local_discriminators[global_discr];
    }

    size_t size() const override
    {
        return offsets->size();
    }

    Field operator[](size_t n) const override;
    void get(size_t n, Field & res) const override;

    bool isDefaultAt(size_t n) const override;
    bool isNullAt(size_t n) const override;
    StringRef getDataAt(size_t n) const override;
    void insertData(const char * pos, size_t length) override;
    void insert(const Field & x) override;
    bool tryInsert(const Field & x) override;
    void insertIntoVariant(const Field & x, Discriminator global_discr);
    void insertFrom(const IColumn & src_, size_t n) override;
    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;
    void insertManyFrom(const IColumn & src, size_t position, size_t length) override;
    void insertDefault() override;
    void insertManyDefaults(size_t length) override;
    void popBack(size_t n) override;
    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin, const UInt8 *) const override;
    const char * deserializeAndInsertFromArena(const char * pos) override;
    const char * skipSerializedInArena(const char * pos) const override;
    void updateHashWithValue(size_t n, SipHash & hash) const override;
    void updateWeakHash32(WeakHash32 & hash) const override;
    void updateHashFast(SipHash & hash) const override;
    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override;
    void expand(const Filter & mask, bool inverted) override;
    ColumnPtr permute(const Permutation & perm, size_t limit) const override;
    ColumnPtr index(const IColumn & indexes, size_t limit) const override;
    template <typename Type>
    ColumnPtr indexImpl(const PaddedPODArray<Type> & indexes, size_t limit) const;
    ColumnPtr replicate(const Offsets & replicate_offsets) const override;
    MutableColumns scatter(ColumnIndex num_columns, const Selector & selector) const override;
    void gather(ColumnGathererStream & gatherer_stream) override;

    int compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override;
    void compareColumn(const IColumn & rhs, size_t rhs_row_num,
                               PaddedPODArray<UInt64> * row_indexes, PaddedPODArray<Int8> & compare_results,
                               int direction, int nan_direction_hint) const override;

    bool hasEqualValues() const override;
    void getExtremes(Field & min, Field & max) const override;
    void getPermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                        size_t limit, int nan_direction_hint, IColumn::Permutation & res) const override;

    void updatePermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                           size_t limit, int nan_direction_hint, IColumn::Permutation & res, EqualRanges & equal_ranges) const override;

    void reserve(size_t n) override;
    void ensureOwnership() override;
    size_t byteSize() const override;
    size_t byteSizeAt(size_t n) const override;
    size_t allocatedBytes() const override;
    void protect() override;
    void forEachSubcolumn(MutableColumnCallback callback) override;
    void forEachSubcolumnRecursively(RecursiveMutableColumnCallback callback) override;
    bool structureEquals(const IColumn & rhs) const override;
    ColumnPtr compress() const override;
    double getRatioOfDefaultRows(double sample_ratio) const override;
    UInt64 getNumberOfDefaultRows() const override;
    void getIndicesOfNonDefaultRows(Offsets & indices, size_t from, size_t limit) const override;
    void finalize() override;
    bool isFinalized() const override;

    const IColumn & getVariantByLocalDiscriminator(size_t discr) const { return *variants[discr]; }
    const IColumn & getVariantByGlobalDiscriminator(size_t discr) const { return *variants[global_to_local_discriminators.at(discr)]; }
    IColumn & getVariantByLocalDiscriminator(size_t discr) { return *variants[discr]; }
    IColumn & getVariantByGlobalDiscriminator(size_t discr) { return *variants[global_to_local_discriminators.at(discr)]; }

    const ColumnPtr & getVariantPtrByLocalDiscriminator(size_t discr) const { return variants[discr]; }
    const ColumnPtr & getVariantPtrByGlobalDiscriminator(size_t discr) const { return variants[global_to_local_discriminators.at(discr)]; }
    ColumnPtr & getVariantPtrByLocalDiscriminator(size_t discr) { return variants[discr]; }
    ColumnPtr & getVariantPtrByGlobalDiscriminator(size_t discr) { return variants[global_to_local_discriminators.at(discr)]; }

    const IColumn & getLocalDiscriminatorsColumn() const { return *local_discriminators; }
    IColumn & getLocalDiscriminatorsColumn() { return *local_discriminators; }

    const ColumnPtr & getLocalDiscriminatorsPtr() const { return local_discriminators; }
    ColumnPtr & getLocalDiscriminatorsPtr() { return local_discriminators; }

    const Discriminators & ALWAYS_INLINE getLocalDiscriminators() const { return assert_cast<const ColumnDiscriminators &>(*local_discriminators).getData(); }
    Discriminators & ALWAYS_INLINE getLocalDiscriminators() { return assert_cast<ColumnDiscriminators &>(*local_discriminators).getData(); }

    const IColumn & getOffsetsColumn() const { return *offsets; }
    IColumn & getOffsetsColumn() { return *offsets; }

    const ColumnPtr & getOffsetsPtr() const { return offsets; }
    ColumnPtr & getOffsetsPtr() { return offsets; }

    const Offsets & ALWAYS_INLINE getOffsets() const { return assert_cast<const ColumnOffsets &>(*offsets).getData(); }
    Offsets & ALWAYS_INLINE getOffsets() { return assert_cast<ColumnOffsets &>(*offsets).getData(); }

    size_t getNumVariants() const { return variants.size(); }

    bool hasOnlyNulls() const
    {
        /// If all variants are empty, we have only NULL values.
        return std::all_of(variants.begin(), variants.end(), [](const WrappedPtr & v){ return v->empty(); });
    }

    /// Check if local and global order is the same.
    bool hasGlobalVariantsOrder() const
    {
        for (size_t i = 0; i != local_to_global_discriminators.size(); ++i)
        {
            if (local_to_global_discriminators[i] != i)
                return false;
        }

        return true;
    }

    /// Check if we have only 1 non-empty variant and no NULL values,
    /// and if so, return the discriminator of this non-empty column.
    std::optional<Discriminator> getLocalDiscriminatorOfOneNoneEmptyVariantNoNulls() const;

    /// Apply null map to a Variant column.
    /// Replace corresponding discriminators with NULL_DISCRIMINATOR
    /// and filter out rows in variants if needed.
    void applyNullMap(const ColumnVector<UInt8>::Container & null_map);
    void applyNegatedNullMap(const ColumnVector<UInt8>::Container & null_map);

private:
    void initIdentityGlobalToLocalDiscriminatorsMapping();

    template <bool inverted>
    void applyNullMapImpl(const ColumnVector<UInt8>::Container & null_map);

    WrappedPtr local_discriminators;
    WrappedPtr offsets;
    NestedColumns variants;

    std::vector<Discriminator> global_to_local_discriminators;
    std::vector<Discriminator> local_to_global_discriminators;
};


}
