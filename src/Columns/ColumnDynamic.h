#pragma once

#include <Columns/IColumn.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnVariant.h>
#include <DataTypes/IDataType.h>


namespace DB
{

/**
 * Column for storing Dynamic type values.
 * Dynamic column allows to insert and store values of any data types inside.
 * Inside it stores:
 *   - Variant column with all inserted values of different types.
 *   - Information about currently stored variants.
 *
 * When new values are inserted into Dynamic column, the internal Variant
 * type and column are extended if the inserted value has new type.
 */
class ColumnDynamic final : public COWHelper<IColumnHelper<ColumnDynamic>, ColumnDynamic>
{
public:
    ///
    struct Statistics
    {
        enum class Source
        {
            READ,  /// Statistics were loaded into column during reading from MergeTree.
            MERGE, /// Statistics were calculated during merge of several MergeTree parts.
        };

        /// Source of the statistics.
        Source source;
        /// Statistics data: (variant name) -> (total variant size in data part).
        std::unordered_map<String, size_t> data;
    };

private:
    friend class COWHelper<IColumnHelper<ColumnDynamic>, ColumnDynamic>;

    struct VariantInfo
    {
        DataTypePtr variant_type;
        /// Name of the whole variant to not call getName() every time.
        String variant_name;
        /// Names of variants to not call getName() every time on variants.
        Names variant_names;
        /// Mapping (variant name) -> (global discriminator).
        /// It's used during variant extension.
        std::unordered_map<String, UInt8> variant_name_to_discriminator;
    };

    explicit ColumnDynamic(size_t max_dynamic_types_);
    ColumnDynamic(MutableColumnPtr variant_column_, const VariantInfo & variant_info_, size_t max_dynamic_types_, const Statistics & statistics_ = {});

public:
    /** Create immutable column using immutable arguments. This arguments may be shared with other columns.
      * Use IColumn::mutate in order to make mutable column and mutate shared nested columns.
      */
    using Base = COWHelper<IColumnHelper<ColumnDynamic>, ColumnDynamic>;
    static Ptr create(const ColumnPtr & variant_column_, const VariantInfo & variant_info_, size_t max_dynamic_types_, const Statistics & statistics_ = {})
    {
        return ColumnDynamic::create(variant_column_->assumeMutable(), variant_info_, max_dynamic_types_, statistics_);
    }

    static MutablePtr create(MutableColumnPtr variant_column_, const VariantInfo & variant_info_, size_t max_dynamic_types_, const Statistics & statistics_ = {})
    {
        return Base::create(std::move(variant_column_), variant_info_, max_dynamic_types_, statistics_);
    }

    static MutablePtr create(MutableColumnPtr variant_column_, const DataTypePtr & variant_type, size_t max_dynamic_types_, const Statistics & statistics_ = {});

    static ColumnPtr create(ColumnPtr variant_column_, const DataTypePtr & variant_type, size_t max_dynamic_types_, const Statistics & statistics_ = {})
    {
        return create(variant_column_->assumeMutable(), variant_type, max_dynamic_types_, statistics_);
    }

    static MutablePtr create(size_t max_dynamic_types_)
    {
        return Base::create(max_dynamic_types_);
    }

    std::string getName() const override { return "Dynamic(max_types=" + std::to_string(max_dynamic_types) + ")"; }

    const char * getFamilyName() const override
    {
        return "Dynamic";
    }

    TypeIndex getDataType() const override
    {
        return TypeIndex::Dynamic;
    }

    MutableColumnPtr cloneEmpty() const override
    {
        /// Keep current dynamic structure
        return Base::create(variant_column->cloneEmpty(), variant_info, max_dynamic_types, statistics);
    }

    MutableColumnPtr cloneResized(size_t size) const override
    {
        return Base::create(variant_column->cloneResized(size), variant_info, max_dynamic_types, statistics);
    }

    size_t size() const override
    {
        return variant_column->size();
    }

    Field operator[](size_t n) const override
    {
        return (*variant_column)[n];
    }

    void get(size_t n, Field & res) const override
    {
        variant_column->get(n, res);
    }

    bool isDefaultAt(size_t n) const override
    {
        return variant_column->isDefaultAt(n);
    }

    bool isNullAt(size_t n) const override
    {
        return variant_column->isNullAt(n);
    }

    StringRef getDataAt(size_t n) const override
    {
        return variant_column->getDataAt(n);
    }

    void insertData(const char * pos, size_t length) override
    {
        variant_column->insertData(pos, length);
    }

    void insert(const Field & x) override;
    bool tryInsert(const Field & x) override;
    void insertFrom(const IColumn & src_, size_t n) override;
    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;
    void insertManyFrom(const IColumn & src, size_t position, size_t length) override;

    void insertDefault() override
    {
        variant_column->insertDefault();
    }

    void insertManyDefaults(size_t length) override
    {
        variant_column->insertManyDefaults(length);
    }

    void popBack(size_t n) override
    {
        variant_column->popBack(n);
    }

    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override;
    const char * deserializeAndInsertFromArena(const char * pos) override;
    const char * skipSerializedInArena(const char * pos) const override;

    void updateHashWithValue(size_t n, SipHash & hash) const override;

    void updateWeakHash32(WeakHash32 & hash) const override
    {
        variant_column->updateWeakHash32(hash);
    }

    void updateHashFast(SipHash & hash) const override
    {
        variant_column->updateHashFast(hash);
    }

    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override
    {
        return create(variant_column->filter(filt, result_size_hint), variant_info, max_dynamic_types);
    }

    void expand(const Filter & mask, bool inverted) override
    {
        variant_column->expand(mask, inverted);
    }

    ColumnPtr permute(const Permutation & perm, size_t limit) const override
    {
        return create(variant_column->permute(perm, limit), variant_info, max_dynamic_types);
    }

    ColumnPtr index(const IColumn & indexes, size_t limit) const override
    {
        return create(variant_column->index(indexes, limit), variant_info, max_dynamic_types);
    }

    ColumnPtr replicate(const Offsets & replicate_offsets) const override
    {
        return create(variant_column->replicate(replicate_offsets), variant_info, max_dynamic_types);
    }

    MutableColumns scatter(ColumnIndex num_columns, const Selector & selector) const override
    {
        MutableColumns scattered_variant_columns = variant_column->scatter(num_columns, selector);
        MutableColumns scattered_columns;
        scattered_columns.reserve(num_columns);
        for (auto & scattered_variant_column : scattered_variant_columns)
            scattered_columns.emplace_back(create(std::move(scattered_variant_column), variant_info, max_dynamic_types));

        return scattered_columns;
    }

    int compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override;

    bool hasEqualValues() const override
    {
        return variant_column->hasEqualValues();
    }

    void getExtremes(Field & min, Field & max) const override
    {
        variant_column->getExtremes(min, max);
    }

    void getPermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                        size_t limit, int nan_direction_hint, IColumn::Permutation & res) const override
    {
        variant_column->getPermutation(direction, stability, limit, nan_direction_hint, res);
    }

    void updatePermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                           size_t limit, int nan_direction_hint, IColumn::Permutation & res, EqualRanges & equal_ranges) const override
    {
        variant_column->updatePermutation(direction, stability, limit, nan_direction_hint, res, equal_ranges);
    }

    void reserve(size_t n) override
    {
        variant_column->reserve(n);
    }

    void ensureOwnership() override
    {
        variant_column->ensureOwnership();
    }

    size_t byteSize() const override
    {
        return variant_column->byteSize();
    }

    size_t byteSizeAt(size_t n) const override
    {
        return variant_column->byteSizeAt(n);
    }

    size_t allocatedBytes() const override
    {
        return variant_column->allocatedBytes();
    }

    void protect() override
    {
        variant_column->protect();
    }

    void forEachSubcolumn(MutableColumnCallback callback) override
    {
        callback(variant_column);
    }

    void forEachSubcolumnRecursively(RecursiveMutableColumnCallback callback) override
    {
        callback(*variant_column);
        variant_column->forEachSubcolumnRecursively(callback);
    }

    bool structureEquals(const IColumn & rhs) const override
    {
        if (const auto * rhs_concrete = typeid_cast<const ColumnDynamic *>(&rhs))
            return max_dynamic_types == rhs_concrete->max_dynamic_types;
        return false;
    }

    ColumnPtr compress() const override;

    double getRatioOfDefaultRows(double sample_ratio) const override
    {
        return variant_column->getRatioOfDefaultRows(sample_ratio);
    }

    UInt64 getNumberOfDefaultRows() const override
    {
        return variant_column->getNumberOfDefaultRows();
    }

    void getIndicesOfNonDefaultRows(Offsets & indices, size_t from, size_t limit) const override
    {
        variant_column->getIndicesOfNonDefaultRows(indices, from, limit);
    }

    void finalize() override
    {
        variant_column->finalize();
    }

    bool isFinalized() const override
    {
        return variant_column->isFinalized();
    }

    /// Apply null map to a nested Variant column.
    void applyNullMap(const ColumnVector<UInt8>::Container & null_map);
    void applyNegatedNullMap(const ColumnVector<UInt8>::Container & null_map);

    const VariantInfo & getVariantInfo() const { return variant_info; }

    const ColumnPtr & getVariantColumnPtr() const { return variant_column; }
    ColumnPtr & getVariantColumnPtr() { return variant_column; }

    const ColumnVariant & getVariantColumn() const { return assert_cast<const ColumnVariant &>(*variant_column); }
    ColumnVariant & getVariantColumn() { return assert_cast<ColumnVariant &>(*variant_column); }

    bool addNewVariant(const DataTypePtr & new_variant);
    void addStringVariant();

    bool hasDynamicStructure() const override { return true; }
    void takeDynamicStructureFromSourceColumns(const Columns & source_columns) override;

    const Statistics & getStatistics() const { return statistics; }

    size_t getMaxDynamicTypes() const { return max_dynamic_types; }

private:
    /// Combine current variant with the other variant and return global discriminators mapping
    /// from other variant to the combined one. It's used for inserting from
    /// different variants.
    /// Returns nullptr if maximum number of variants is reached and the new variant cannot be created.
    std::vector<UInt8> * combineVariants(const VariantInfo & other_variant_info);

    void updateVariantInfoAndExpandVariantColumn(const DataTypePtr & new_variant_type);

    WrappedPtr variant_column;
    /// Store the type of current variant with some additional information.
    VariantInfo variant_info;
    /// The maximum number of different types that can be stored in this Dynamic column.
    /// If exceeded, all new variants will be converted to String.
    size_t max_dynamic_types;

    /// Size statistics of each variants from MergeTree data part.
    /// Used in takeDynamicStructureFromSourceColumns and set during deserialization.
    Statistics statistics;

    /// Cache (Variant name) -> (global discriminators mapping from this variant to current variant in Dynamic column).
    /// Used to avoid mappings recalculation in combineVariants for the same Variant types.
    std::unordered_map<String, std::vector<UInt8>> variant_mappings_cache;
    /// Cache of Variant types that couldn't be combined with current variant in Dynamic column.
    /// Used to avoid checking if combination is possible for the same Variant types.
    std::unordered_set<String> variants_with_failed_combination;
};

}
