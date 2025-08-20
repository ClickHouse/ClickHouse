#pragma once

#include <Columns/IColumn.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnString.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/SerializationDynamic.h>
#include <Common/StringHashForHeterogeneousLookup.h>
#include <Common/WeakHash.h>

namespace DB
{

class ColumnObject final : public COWHelper<IColumnHelper<ColumnObject>, ColumnObject>
{
public:
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
        /// Statistics for dynamic paths: (path) -> (total number of not-null values).
        std::unordered_map<String, size_t> dynamic_paths_statistics;
        /// Statistics for paths in shared data: path) -> (total number of not-null values).
        /// We don't store statistics for all paths in shared data but only for some subset of them
        /// (is 10000 a good limit? It should not be expensive to store 10000 paths per part)
        static const size_t MAX_SHARED_DATA_STATISTICS_SIZE = 10000;
        std::unordered_map<String, size_t, StringHashForHeterogeneousLookup, StringHashForHeterogeneousLookup::transparent_key_equal> shared_data_paths_statistics;
    };

    using StatisticsPtr = std::shared_ptr<const Statistics>;

    struct ComparatorBase;

    using ComparatorAscendingUnstable = ComparatorAscendingUnstableImpl<ComparatorBase>;
    using ComparatorAscendingStable = ComparatorAscendingStableImpl<ComparatorBase>;
    using ComparatorDescendingUnstable = ComparatorDescendingUnstableImpl<ComparatorBase>;
    using ComparatorDescendingStable = ComparatorDescendingStableImpl<ComparatorBase>;
    using ComparatorEqual = ComparatorEqualImpl<ComparatorBase>;

    struct ComparatorCollationBase;

    using ComparatorCollationAscendingUnstable = ComparatorAscendingUnstableImpl<ComparatorCollationBase>;
    using ComparatorCollationAscendingStable = ComparatorAscendingStableImpl<ComparatorCollationBase>;
    using ComparatorCollationDescendingUnstable = ComparatorDescendingUnstableImpl<ComparatorCollationBase>;
    using ComparatorCollationDescendingStable = ComparatorDescendingStableImpl<ComparatorCollationBase>;
    using ComparatorCollationEqual = ComparatorEqualImpl<ComparatorCollationBase>;

private:
    friend class COWHelper<IColumnHelper<ColumnObject>, ColumnObject>;

    ColumnObject(std::unordered_map<String, MutableColumnPtr> typed_paths_, size_t max_dynamic_paths_, size_t max_dynamic_types_);
    ColumnObject(
        std::unordered_map<String, MutableColumnPtr> typed_paths_,
        std::unordered_map<String, MutableColumnPtr> dynamic_paths_,
        MutableColumnPtr shared_data_,
        size_t max_dynamic_paths_,
        size_t global_max_dynamic_paths_,
        size_t max_dynamic_types_,
        const StatisticsPtr & statistics_ = {});

    ColumnObject(const ColumnObject & other);

    /// Use StringHashForHeterogeneousLookup hash for hash maps to be able to use std::string_view in find() method.
    using PathToColumnMap = std::unordered_map<String, WrappedPtr, StringHashForHeterogeneousLookup, StringHashForHeterogeneousLookup::transparent_key_equal>;
    using PathToDynamicColumnPtrMap = std::unordered_map<String, ColumnDynamic *, StringHashForHeterogeneousLookup, StringHashForHeterogeneousLookup::transparent_key_equal>;
public:
    /** Create immutable column using immutable arguments. This arguments may be shared with other columns.
      * Use mutate in order to make mutable column and mutate shared nested columns.
      */
    using Base = COWHelper<IColumnHelper<ColumnObject>, ColumnObject>;

    static Ptr create(
        const std::unordered_map<String, ColumnPtr> & typed_paths_,
        const std::unordered_map<String, ColumnPtr> & dynamic_paths_,
        const ColumnPtr & shared_data_,
        size_t max_dynamic_paths_,
        size_t global_max_dynamic_paths_,
        size_t max_dynamic_types_,
        const StatisticsPtr & statistics_ = {});

    static MutablePtr create(
        std::unordered_map<String, MutableColumnPtr> typed_paths_,
        std::unordered_map<String, MutableColumnPtr> dynamic_paths_,
        MutableColumnPtr shared_data_,
        size_t max_dynamic_paths_,
        size_t global_max_dynamic_paths_,
        size_t max_dynamic_types_,
        const StatisticsPtr & statistics_ = {});

    static MutablePtr create(std::unordered_map<String, MutableColumnPtr> typed_paths_, size_t max_dynamic_paths_, size_t max_dynamic_types_);

    std::string getName() const override;

    const char * getFamilyName() const override
    {
        return "Object";
    }

    TypeIndex getDataType() const override
    {
        return TypeIndex::Object;
    }

    MutableColumnPtr cloneEmpty() const override;
    MutableColumnPtr cloneResized(size_t size) const override;

    size_t size() const override
    {
        return shared_data->size();
    }

    Field operator[](size_t n) const override;
    void get(size_t n, Field & res) const override;
    std::pair<String, DataTypePtr> getValueNameAndType(size_t n) const override;

    bool isDefaultAt(size_t n) const override;
    StringRef getDataAt(size_t n) const override;
    void insertData(const char * pos, size_t length) override;

    void insert(const Field & x) override;
    bool tryInsert(const Field & x) override;
#if !defined(DEBUG_OR_SANITIZER_BUILD)
    void insertFrom(const IColumn & src, size_t n) override;
    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;
#else
    void doInsertFrom(const IColumn & src, size_t n) override;
    void doInsertRangeFrom(const IColumn & src, size_t start, size_t length) override;
#endif
    /// TODO: implement more optimal insertManyFrom
    void insertDefault() override;
    void insertManyDefaults(size_t length) override;

    void popBack(size_t n) override;

    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override;
    const char * deserializeAndInsertFromArena(const char * pos) override;
    const char * skipSerializedInArena(const char * pos) const override;

    void updateHashWithValue(size_t n, SipHash & hash) const override;
    WeakHash32 getWeakHash32() const override;
    void updateHashFast(SipHash & hash) const override;

    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override;
    void expand(const Filter & mask, bool inverted) override;
    ColumnPtr permute(const Permutation & perm, size_t limit) const override;
    ColumnPtr index(const IColumn & indexes, size_t limit) const override;
    ColumnPtr replicate(const Offsets & replicate_offsets) const override;
    MutableColumns scatter(ColumnIndex num_columns, const Selector & selector) const override;

    void getPermutation(PermutationSortDirection direction, PermutationSortStability stability,
                        size_t limit, int nan_direction_hint, Permutation & res) const override;
    void updatePermutation(PermutationSortDirection direction, PermutationSortStability stability,
                           size_t limit, int nan_direction_hint, Permutation & res, EqualRanges & equal_ranges) const override;

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    int compareAt(size_t, size_t, const IColumn &, int nan_direction_hint) const override;
#else
    int doCompareAt(size_t, size_t, const IColumn &, int nan_direction_hint) const override;
#endif
    void getExtremes(Field & min, Field & max) const override;

    void reserve(size_t n) override;
    size_t capacity() const override;
    void prepareForSquashing(const std::vector<ColumnPtr> & source_columns, size_t factor) override;
    void ensureOwnership() override;
    size_t byteSize() const override;
    size_t byteSizeAt(size_t n) const override;
    size_t allocatedBytes() const override;
    void protect() override;
    ColumnCheckpointPtr getCheckpoint() const override;
    void updateCheckpoint(ColumnCheckpoint & checkpoint) const override;
    void rollback(const ColumnCheckpoint & checkpoint) override;

    void forEachMutableSubcolumn(MutableColumnCallback callback) override;

    void forEachMutableSubcolumnRecursively(RecursiveMutableColumnCallback callback) override;

    void forEachSubcolumn(ColumnCallback callback) const override;

    void forEachSubcolumnRecursively(RecursiveColumnCallback callback) const override;

    bool structureEquals(const IColumn & rhs) const override;

    ColumnPtr compress(bool force_compression) const override;

    void finalize() override;
    bool isFinalized() const override;
    bool canBeInsideNullable() const override { return true; }

    bool hasDynamicStructure() const override { return true; }
    bool dynamicStructureEquals(const IColumn & rhs) const override;
    void takeDynamicStructureFromSourceColumns(const Columns & source_columns) override;

    const PathToColumnMap & getTypedPaths() const { return typed_paths; }
    PathToColumnMap & getTypedPaths() { return typed_paths; }

    const PathToColumnMap & getDynamicPaths() const { return dynamic_paths; }
    PathToColumnMap & getDynamicPaths() { return dynamic_paths; }

    const PathToDynamicColumnPtrMap & getDynamicPathsPtrs() const { return dynamic_paths_ptrs; }
    PathToDynamicColumnPtrMap & getDynamicPathsPtrs() { return dynamic_paths_ptrs; }

    const StatisticsPtr & getStatistics() const { return statistics; }

    const ColumnPtr & getSharedDataPtr() const { return shared_data; }
    ColumnPtr & getSharedDataPtr() { return shared_data; }
    IColumn & getSharedDataColumn() { return *shared_data; }

    const ColumnArray & getSharedDataNestedColumn() const { return assert_cast<const ColumnArray &>(*shared_data); }
    ColumnArray & getSharedDataNestedColumn() { return assert_cast<ColumnArray &>(*shared_data); }

    ColumnArray::Offsets & getSharedDataOffsets() { return assert_cast<ColumnArray &>(*shared_data).getOffsets(); }
    const ColumnArray::Offsets & getSharedDataOffsets() const { return assert_cast<const ColumnArray &>(*shared_data).getOffsets(); }

    std::pair<ColumnString *, ColumnString *> getSharedDataPathsAndValues()
    {
        auto & column_array = assert_cast<ColumnArray &>(*shared_data);
        auto & column_tuple = assert_cast<ColumnTuple &>(column_array.getData());
        return {assert_cast<ColumnString *>(&column_tuple.getColumn(0)), assert_cast<ColumnString *>(&column_tuple.getColumn(1))};
    }

    std::pair<const ColumnString *, const ColumnString *> getSharedDataPathsAndValues() const
    {
        const auto & column_array = assert_cast<const ColumnArray &>(*shared_data);
        const auto & column_tuple = assert_cast<const ColumnTuple &>(column_array.getData());
        return {assert_cast<const ColumnString *>(&column_tuple.getColumn(0)), assert_cast<const ColumnString *>(&column_tuple.getColumn(1))};
    }

    size_t getMaxDynamicTypes() const { return max_dynamic_types; }
    size_t getMaxDynamicPaths() const { return max_dynamic_paths; }
    size_t getGlobalMaxDynamicPaths() const { return global_max_dynamic_paths; }

    /// Try to add new dynamic path. Returns pointer to the new dynamic
    /// path column or nullptr if limit on dynamic paths is reached.
    ColumnDynamic * tryToAddNewDynamicPath(std::string_view path);
    /// Throws an exception if cannot add.
    void addNewDynamicPath(std::string_view path);
    void addNewDynamicPath(std::string_view path, MutableColumnPtr column);
    bool canAddNewDynamicPath() const { return dynamic_paths.size() < max_dynamic_paths; }

    void setDynamicPaths(const std::vector<String> & paths);
    void setDynamicPaths(const std::vector<std::pair<String, ColumnPtr>> & paths);
    void setMaxDynamicPaths(size_t max_dynamic_paths_);
    void setGlobalMaxDynamicPaths(size_t global_max_dynamic_paths_);
    void setStatistics(const StatisticsPtr & statistics_) { statistics = statistics_; }

    static void serializePathAndValueIntoSharedData(ColumnString * shared_data_paths, ColumnString * shared_data_values, std::string_view path, const IColumn & column, size_t n);
    static void deserializeValueFromSharedData(const ColumnString * shared_data_values, size_t n, IColumn & column);

    /// Paths in shared data are sorted in each row. Use this method to find the lower bound for specific path in the row.
    static size_t findPathLowerBoundInSharedData(StringRef path, const ColumnString & shared_data_paths, size_t start, size_t end);
    /// Insert all the data from shared data with specified path to dynamic column.
    static void fillPathColumnFromSharedData(IColumn & path_column, StringRef path, const ColumnPtr & shared_data_column, size_t start, size_t end);

private:
    class SortedPathsIterator;

    void insertFromSharedDataAndFillRemainingDynamicPaths(const ColumnObject & src_object_column, std::vector<std::string_view> && src_dynamic_paths_for_shared_data, size_t start, size_t length);
    void serializePathAndValueIntoArena(Arena & arena, const char *& begin, StringRef path, StringRef value, StringRef & res) const;

    /// Map path -> column for paths with explicitly specified types.
    /// This set of paths is constant and cannot be changed.
    PathToColumnMap typed_paths;
    /// Sorted list of typed paths. Used to avoid sorting paths every time in some methods.
    std::vector<std::string_view> sorted_typed_paths;
    /// Map path -> column for dynamically added paths. All columns
    /// here are Dynamic columns. This set of paths can be extended
    /// during inerts into the column.
    PathToColumnMap dynamic_paths;
    /// Sorted list of dynamic paths. Used to avoid sorting paths every time in some methods.
    std::set<std::string_view> sorted_dynamic_paths;

    /// Store and use pointers to ColumnDynamic to avoid virtual calls.
    /// With hundreds of dynamic paths these virtual calls are noticeable.
    PathToDynamicColumnPtrMap dynamic_paths_ptrs;
    /// Shared storage for all other paths and values. It's filled
    /// when the number of dynamic paths reaches the limit.
    /// It has type Array(Tuple(String, String)) and stores
    /// an array of pairs (path, binary serialized dynamic value) for each row.
    WrappedPtr shared_data;

    /// Maximum number of dynamic paths. If this limit is reached, all new paths will be inserted into shared data.
    /// This limit can be different for different instances of Object column. For example, we can decrease it
    /// in takeDynamicStructureFromSourceColumns before merge.
    size_t max_dynamic_paths;
    /// Global limit on number of dynamic paths for all column instances of this Object type. It's the limit specified
    /// in the type definition (for example 'JSON(max_dynamic_paths=N)'). max_dynamic_paths is always not greater than this limit.
    size_t global_max_dynamic_paths;
    /// Maximum number of dynamic types for each dynamic path. Used while creating Dynamic columns for new dynamic paths.
    size_t max_dynamic_types;
    /// Statistics on the number of non-null values for each dynamic path and for some shared data paths in the MergeTree data part.
    /// Calculated during serializing of data part in MergeTree. Used to determine the set of dynamic paths for the merged part.
    StatisticsPtr statistics;
};

}
