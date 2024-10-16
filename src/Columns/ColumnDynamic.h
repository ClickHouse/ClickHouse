#pragma once

#include <Columns/IColumn.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnString.h>
#include <DataTypes/IDataType.h>
#include <Common/WeakHash.h>


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
 * When the limit on number of dynamic types is exceeded, all values
 * with new types are inserted into special shared variant with type String
 * that contains values and their types in binary format.
 */
class ColumnDynamic final : public COWHelper<IColumnHelper<ColumnDynamic>, ColumnDynamic>
{
public:
    /// Maximum limit on dynamic types. We use ColumnVariant to store all the types,
    /// so the limit cannot be greater then ColumnVariant::MAX_NESTED_COLUMNS.
    /// We also always have reserved variant for shared variant.
    static constexpr size_t MAX_DYNAMIC_TYPES_LIMIT = ColumnVariant::MAX_NESTED_COLUMNS - 1;
    static constexpr const char * SHARED_VARIANT_TYPE_NAME = "SharedVariant";

    struct Statistics
    {
        enum class Source
        {
            READ,  /// Statistics were loaded into column during reading from MergeTree.
            MERGE, /// Statistics were calculated during merge of several MergeTree parts.
        };

        explicit Statistics(Source source_) : source(source_) {}

        /// Source of the statistics.
        Source source;
        /// Statistics data for usual variants: (variant name) -> (total variant size in data part).
        std::unordered_map<String, size_t> variants_statistics;
        /// Statistics data for variants from shared variant: (variant name) -> (total variant size in data part).
        /// For shared variant we store statistics only for first 256 variants (should cover almost all cases and it's not expensive).
        static constexpr const size_t MAX_SHARED_VARIANT_STATISTICS_SIZE = 256;
        std::unordered_map<String, size_t> shared_variants_statistics;
    };

    using StatisticsPtr = std::shared_ptr<const Statistics>;

    struct ComparatorBase;
    using ComparatorAscendingUnstable = ComparatorAscendingUnstableImpl<ComparatorBase>;
    using ComparatorAscendingStable = ComparatorAscendingStableImpl<ComparatorBase>;
    using ComparatorDescendingUnstable = ComparatorDescendingUnstableImpl<ComparatorBase>;
    using ComparatorDescendingStable = ComparatorDescendingStableImpl<ComparatorBase>;
    using ComparatorEqual = ComparatorEqualImpl<ComparatorBase>;

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
    ColumnDynamic(MutableColumnPtr variant_column_, const DataTypePtr & variant_type_, size_t max_dynamic_types_, size_t global_max_dynamic_types_, const StatisticsPtr & statistics_ = {});
    ColumnDynamic(MutableColumnPtr variant_column_, const VariantInfo & variant_info_, size_t max_dynamic_types_, size_t global_max_dynamic_types_, const StatisticsPtr & statistics_ = {});

public:
    /** Create immutable column using immutable arguments. This arguments may be shared with other columns.
      * Use IColumn::mutate in order to make mutable column and mutate shared nested columns.
      */
    using Base = COWHelper<IColumnHelper<ColumnDynamic>, ColumnDynamic>;
    static Ptr create(const ColumnPtr & variant_column_, const VariantInfo & variant_info_, size_t max_dynamic_types_, size_t global_max_dynamic_types_, const StatisticsPtr & statistics_ = {})
    {
        return ColumnDynamic::create(variant_column_->assumeMutable(), variant_info_, max_dynamic_types_, global_max_dynamic_types_, statistics_);
    }

    static MutablePtr create(MutableColumnPtr variant_column_, const VariantInfo & variant_info_, size_t max_dynamic_types_, size_t global_max_dynamic_types_, const StatisticsPtr & statistics_ = {})
    {
        return Base::create(std::move(variant_column_), variant_info_, max_dynamic_types_, global_max_dynamic_types_, statistics_);
    }

    static MutablePtr create(MutableColumnPtr variant_column_, const DataTypePtr & variant_type_, size_t max_dynamic_types_, size_t global_max_dynamic_types_, const StatisticsPtr & statistics_ = {})
    {
        return Base::create(std::move(variant_column_), variant_type_, max_dynamic_types_, global_max_dynamic_types_, statistics_);
    }

    static ColumnPtr create(ColumnPtr variant_column_, const DataTypePtr & variant_type, size_t max_dynamic_types_, size_t global_max_dynamic_types_, const StatisticsPtr & statistics_ = {})
    {
        return create(variant_column_->assumeMutable(), variant_type, max_dynamic_types_, global_max_dynamic_types_, statistics_);
    }

    static MutablePtr create(size_t max_dynamic_types_ = MAX_DYNAMIC_TYPES_LIMIT)
    {
        return Base::create(max_dynamic_types_);
    }

    std::string getName() const override { return "Dynamic(max_types=" + std::to_string(global_max_dynamic_types) + ")"; }

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
        return Base::create(variant_column->cloneEmpty(), variant_info, max_dynamic_types, global_max_dynamic_types, statistics);
    }

    MutableColumnPtr cloneResized(size_t size) const override
    {
        return Base::create(variant_column->cloneResized(size), variant_info, max_dynamic_types, global_max_dynamic_types, statistics);
    }

    size_t size() const override
    {
        return variant_column_ptr->size();
    }

    Field operator[](size_t n) const override;

    void get(size_t n, Field & res) const override;

    bool isDefaultAt(size_t n) const override
    {
        return variant_column_ptr->isDefaultAt(n);
    }

    bool isNullAt(size_t n) const override
    {
        return variant_column_ptr->isNullAt(n);
    }

    StringRef getDataAt(size_t n) const override
    {
        return variant_column_ptr->getDataAt(n);
    }

    void insertData(const char * pos, size_t length) override
    {
        variant_column_ptr->insertData(pos, length);
    }

    void insert(const Field & x) override;
    bool tryInsert(const Field & x) override;

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    void insertFrom(const IColumn & src_, size_t n) override;
    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;
    void insertManyFrom(const IColumn & src, size_t position, size_t length) override;
#else
    void doInsertFrom(const IColumn & src_, size_t n) override;
    void doInsertRangeFrom(const IColumn & src, size_t start, size_t length) override;
    void doInsertManyFrom(const IColumn & src, size_t position, size_t length) override;
#endif

    void insertDefault() override
    {
        variant_column_ptr->insertDefault();
    }

    void insertManyDefaults(size_t length) override
    {
        variant_column_ptr->insertManyDefaults(length);
    }

    void popBack(size_t n) override
    {
        variant_column_ptr->popBack(n);
    }

    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override;
    const char * deserializeAndInsertFromArena(const char * pos) override;
    const char * skipSerializedInArena(const char * pos) const override;

    void updateHashWithValue(size_t n, SipHash & hash) const override;

    WeakHash32 getWeakHash32() const override
    {
        return variant_column_ptr->getWeakHash32();
    }

    void updateHashFast(SipHash & hash) const override
    {
        variant_column_ptr->updateHashFast(hash);
    }

    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override
    {
        return create(variant_column_ptr->filter(filt, result_size_hint), variant_info, max_dynamic_types, global_max_dynamic_types);
    }

    void expand(const Filter & mask, bool inverted) override
    {
        variant_column_ptr->expand(mask, inverted);
    }

    ColumnPtr permute(const Permutation & perm, size_t limit) const override
    {
        return create(variant_column_ptr->permute(perm, limit), variant_info, max_dynamic_types, global_max_dynamic_types);
    }

    ColumnPtr index(const IColumn & indexes, size_t limit) const override
    {
        return create(variant_column_ptr->index(indexes, limit), variant_info, max_dynamic_types, global_max_dynamic_types);
    }

    ColumnPtr replicate(const Offsets & replicate_offsets) const override
    {
        return create(variant_column_ptr->replicate(replicate_offsets), variant_info, max_dynamic_types, global_max_dynamic_types);
    }

    MutableColumns scatter(ColumnIndex num_columns, const Selector & selector) const override
    {
        MutableColumns scattered_variant_columns = variant_column_ptr->scatter(num_columns, selector);
        MutableColumns scattered_columns;
        scattered_columns.reserve(num_columns);
        for (auto & scattered_variant_column : scattered_variant_columns)
            scattered_columns.emplace_back(create(std::move(scattered_variant_column), variant_info, max_dynamic_types, global_max_dynamic_types));

        return scattered_columns;
    }

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    int compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override;
#else
    int doCompareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override;
#endif

    bool hasEqualValues() const override
    {
        return variant_column_ptr->hasEqualValues();
    }

    void getExtremes(Field & min, Field & max) const override
    {
        variant_column_ptr->getExtremes(min, max);
    }

    void getPermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                        size_t limit, int nan_direction_hint, IColumn::Permutation & res) const override;

    void updatePermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                           size_t limit, int nan_direction_hint, IColumn::Permutation & res, EqualRanges & equal_ranges) const override;

    void reserve(size_t n) override
    {
        variant_column_ptr->reserve(n);
    }

    size_t capacity() const override
    {
        return variant_column_ptr->capacity();
    }

    void prepareForSquashing(const Columns & source_columns) override;
    /// Prepare only variants but not discriminators and offsets.
    void prepareVariantsForSquashing(const Columns & source_columns);

    void ensureOwnership() override
    {
        variant_column_ptr->ensureOwnership();
    }

    size_t byteSize() const override
    {
        return variant_column_ptr->byteSize();
    }

    size_t byteSizeAt(size_t n) const override
    {
        return variant_column_ptr->byteSizeAt(n);
    }

    size_t allocatedBytes() const override
    {
        return variant_column_ptr->allocatedBytes();
    }

    void protect() override
    {
        variant_column_ptr->protect();
    }

    void forEachSubcolumn(MutableColumnCallback callback) override
    {
        callback(variant_column);
        variant_column_ptr = assert_cast<ColumnVariant *>(variant_column.get());
    }

    void forEachSubcolumnRecursively(RecursiveMutableColumnCallback callback) override
    {
        callback(*variant_column);
        variant_column_ptr = assert_cast<ColumnVariant *>(variant_column.get());
        variant_column->forEachSubcolumnRecursively(callback);
    }

    bool structureEquals(const IColumn & rhs) const override
    {
        if (const auto * rhs_concrete = typeid_cast<const ColumnDynamic *>(&rhs))
            return global_max_dynamic_types == rhs_concrete->global_max_dynamic_types;
        return false;
    }

    ColumnPtr compress() const override;

    double getRatioOfDefaultRows(double sample_ratio) const override
    {
        return variant_column_ptr->getRatioOfDefaultRows(sample_ratio);
    }

    UInt64 getNumberOfDefaultRows() const override
    {
        return variant_column_ptr->getNumberOfDefaultRows();
    }

    void getIndicesOfNonDefaultRows(Offsets & indices, size_t from, size_t limit) const override
    {
        variant_column_ptr->getIndicesOfNonDefaultRows(indices, from, limit);
    }

    void finalize() override
    {
        variant_column_ptr->finalize();
    }

    bool isFinalized() const override
    {
        return variant_column_ptr->isFinalized();
    }

    /// Apply null map to a nested Variant column.
    void applyNullMap(const ColumnVector<UInt8>::Container & null_map);
    void applyNegatedNullMap(const ColumnVector<UInt8>::Container & null_map);

    const VariantInfo & getVariantInfo() const { return variant_info; }

    const ColumnPtr & getVariantColumnPtr() const { return variant_column; }
    ColumnPtr & getVariantColumnPtr() { return variant_column; }

    const ColumnVariant & getVariantColumn() const { return *variant_column_ptr; }
    ColumnVariant & getVariantColumn() { return *variant_column_ptr; }

    bool addNewVariant(const DataTypePtr & new_variant, const String & new_variant_name);
    bool addNewVariant(const DataTypePtr & new_variant) { return addNewVariant(new_variant, new_variant->getName()); }

    bool hasDynamicStructure() const override { return true; }
    void takeDynamicStructureFromSourceColumns(const Columns & source_columns) override;

    const StatisticsPtr & getStatistics() const { return statistics; }
    void setStatistics(const StatisticsPtr & statistics_) { statistics = statistics_; }

    size_t getMaxDynamicTypes() const { return max_dynamic_types; }

    /// Check if we can add new variant types.
    /// Shared variant doesn't count in the limit but always presents,
    /// so we should subtract 1 from the total types count.
    bool canAddNewVariants(size_t current_variants_count, size_t new_variants_count) const { return current_variants_count + new_variants_count - 1 <= max_dynamic_types; }
    bool canAddNewVariant(size_t current_variants_count) const { return canAddNewVariants(current_variants_count, 1); }
    bool canAddNewVariants(size_t new_variants_count) const { return canAddNewVariants(variant_info.variant_names.size(), new_variants_count); }
    bool canAddNewVariant() const { return canAddNewVariants(variant_info.variant_names.size(), 1); }

    void setVariantType(const DataTypePtr & variant_type);
    void setMaxDynamicPaths(size_t max_dynamic_type_);

    static const String & getSharedVariantTypeName()
    {
        static const String name = SHARED_VARIANT_TYPE_NAME;
        return name;
    }

    static DataTypePtr getSharedVariantDataType();

    ColumnVariant::Discriminator getSharedVariantDiscriminator() const
    {
        return variant_info.variant_name_to_discriminator.at(getSharedVariantTypeName());
    }

    ColumnString & getSharedVariant()
    {
        return assert_cast<ColumnString &>(getVariantColumn().getVariantByGlobalDiscriminator(getSharedVariantDiscriminator()));
    }

    const ColumnString & getSharedVariant() const
    {
        return assert_cast<const ColumnString &>(getVariantColumn().getVariantByGlobalDiscriminator(getSharedVariantDiscriminator()));
    }

    /// Serializes type and value in binary format into provided shared variant. Doesn't update Variant discriminators and offsets.
    static void serializeValueIntoSharedVariant(ColumnString & shared_variant, const IColumn & src, const DataTypePtr & type, const SerializationPtr & serialization, size_t n);

    /// Insert value into shared variant. Also updates Variant discriminators and offsets.
    void insertValueIntoSharedVariant(const IColumn & src, const DataTypePtr & type, const String & type_name, size_t n);

    const SerializationPtr & getVariantSerialization(const DataTypePtr & variant_type, const String & variant_name)
    {
        /// Get serialization for provided data type.
        /// To avoid calling type->getDefaultSerialization() every time we use simple cache with max size.
        /// When max size is reached, just clear the cache.
        if (serialization_cache.size() == SERIALIZATION_CACHE_MAX_SIZE)
            serialization_cache.clear();

        if (auto it = serialization_cache.find(variant_name); it != serialization_cache.end())
            return it->second;

        return serialization_cache.emplace(variant_name, variant_type->getDefaultSerialization()).first->second;
    }

    const SerializationPtr & getVariantSerialization(const DataTypePtr & variant_type) { return getVariantSerialization(variant_type, variant_type->getName()); }

    String getTypeNameAt(size_t row_num) const;
    void getAllTypeNamesInto(std::unordered_set<String> & names) const;

private:
    void createVariantInfo(const DataTypePtr & variant_type);

    /// Combine current variant with the other variant and return global discriminators mapping
    /// from other variant to the combined one. It's used for inserting from
    /// different variants.
    /// Returns nullptr if maximum number of variants is reached and the new variant cannot be created.
    std::vector<UInt8> * combineVariants(const VariantInfo & other_variant_info);

    void updateVariantInfoAndExpandVariantColumn(const DataTypePtr & new_variant_type);

    WrappedPtr variant_column;
    /// Store and use pointer to ColumnVariant to avoid virtual calls.
    /// ColumnDynamic is widely used inside ColumnObject for each path and
    /// with hundreds of paths these virtual calls are noticeable.
    ColumnVariant * variant_column_ptr;
    /// Store the type of current variant with some additional information.
    VariantInfo variant_info;
    /// The maximum number of different types that can be stored in this Dynamic column.
    /// If exceeded, all new variants will be added to a special shared variant with type String
    /// in binary format. This limit can be different for different instances of Dynamic column.
    /// When max_dynamic_types = 0, we will have only shared variant and insert all values into it.
    size_t max_dynamic_types;
    /// The types limit specified in the data type by the user Dynamic(max_types=N).
    /// max_dynamic_types in all column instances of this Dynamic type can be only smaller
    /// (for example, max_dynamic_types can be reduced in takeDynamicStructureFromSourceColumns
    /// before merge of different Dynamic columns).
    size_t global_max_dynamic_types;

    /// Size statistics of each variants from MergeTree data part.
    /// Used in takeDynamicStructureFromSourceColumns and set during deserialization.
    StatisticsPtr statistics;

    /// Cache (Variant name) -> (global discriminators mapping from this variant to current variant in Dynamic column).
    /// Used to avoid mappings recalculation in combineVariants for the same Variant types.
    std::unordered_map<String, std::vector<UInt8>> variant_mappings_cache;
    /// Cache of Variant types that couldn't be combined with current variant in Dynamic column.
    /// Used to avoid checking if combination is possible for the same Variant types.
    std::unordered_set<String> variants_with_failed_combination;

    /// We can use serializations of different data types to serialize values into shared variant.
    /// To avoid creating the same serialization multiple times, use simple cache.
    static const size_t SERIALIZATION_CACHE_MAX_SIZE = 256;
    std::unordered_map<String, SerializationPtr> serialization_cache;
};

void extendVariantColumn(
    IColumn & variant_column,
    const DataTypePtr & old_variant_type,
    const DataTypePtr & new_variant_type,
    std::unordered_map<String, UInt8> old_variant_name_to_discriminator);

}
