#pragma once

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <Core/Names.h>
#include <DataTypes/Serializations/SubcolumnsTree.h>
#include <Common/PODArray.h>
#include <Common/WeakHash.h>

#include <DataTypes/IDataType.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

/// Info that represents a scalar or array field in a decomposed view.
/// It allows to recreate field with different number
/// of dimensions or nullability.
struct FieldInfo
{
    /// The common type of of all scalars in field.
    DataTypePtr scalar_type;

    /// Do we have NULL scalar in field.
    bool have_nulls;

    /// If true then we have scalars with different types in array and
    /// we need to convert scalars to the common type.
    bool need_convert;

    /// Number of dimension in array. 0 if field is scalar.
    size_t num_dimensions;

    /// If true then this field is an array of variadic dimension field
    /// and we need to normalize the dimension
    bool need_fold_dimension;
};

FieldInfo getFieldInfo(const Field & field);

/** A column that represents object with dynamic set of subcolumns.
 *  Subcolumns are identified by paths in document and are stored in
 *  a trie-like structure. ColumnObjectDeprecated is not suitable for writing into tables
 *  and it should be converted to Tuple with fixed set of subcolumns before that.
 */
class ColumnObjectDeprecated final : public COWHelper<IColumnHelper<ColumnObjectDeprecated>, ColumnObjectDeprecated>
{
public:
    /** Class that represents one subcolumn.
     * It stores values in several parts of column
     * and keeps current common type of all parts.
     * We add a new column part with a new type, when we insert a field,
     * which can't be converted to the current common type.
     * After insertion of all values subcolumn should be finalized
     * for writing and other operations.
     */
    class Subcolumn
    {
    public:
        Subcolumn() = default;
        Subcolumn(size_t size_, bool is_nullable_);
        Subcolumn(MutableColumnPtr && data_, bool is_nullable_);

        size_t size() const;
        size_t byteSize() const;
        size_t allocatedBytes() const;
        void get(size_t n, Field & res) const;

        bool isFinalized() const;
        const DataTypePtr & getLeastCommonType() const { return least_common_type.get(); }
        const DataTypePtr & getLeastCommonTypeBase() const { return least_common_type.getBase(); }
        size_t getNumberOfDimensions() const { return least_common_type.getNumberOfDimensions(); }

        /// Checks the consistency of column's parts stored in @data.
        void checkTypes() const;

        /// Inserts a field, which scalars can be arbitrary, but number of
        /// dimensions should be consistent with current common type.
        void insert(Field field);
        void insert(Field field, FieldInfo info);

        void insertDefault();
        void insertManyDefaults(size_t length);
        void insertRangeFrom(const Subcolumn & src, size_t start, size_t length);
        void popBack(size_t n);

        Subcolumn cut(size_t start, size_t length) const;

        /// Converts all column's parts to the common type and
        /// creates a single column that stores all values.
        void finalize();

        /// Returns last inserted field.
        Field getLastField() const;

        FieldInfo getFieldInfo() const;

        /// Recreates subcolumn with default scalar values and keeps sizes of arrays.
        /// Used to create columns of type Nested with consistent array sizes.
        Subcolumn recreateWithDefaultValues(const FieldInfo & field_info) const;

        /// Returns single column if subcolumn in finalizes.
        /// Otherwise -- undefined behaviour.
        IColumn & getFinalizedColumn();
        const IColumn & getFinalizedColumn() const;
        const ColumnPtr & getFinalizedColumnPtr() const;

        const std::vector<WrappedPtr> & getData() const { return data; }
        size_t getNumberOfDefaultsInPrefix() const { return num_of_defaults_in_prefix; }

        friend class ColumnObjectDeprecated;

    private:
        class LeastCommonType
        {
        public:
            LeastCommonType();
            explicit LeastCommonType(DataTypePtr type_);

            const DataTypePtr & get() const { return type; }
            const DataTypePtr & getBase() const { return base_type; }
            size_t getNumberOfDimensions() const { return num_dimensions; }

        private:
            DataTypePtr type;
            DataTypePtr base_type;
            size_t num_dimensions = 0;
        };

        void addNewColumnPart(DataTypePtr type);

        /// Current least common type of all values inserted to this subcolumn.
        LeastCommonType least_common_type;

        /// If true then common type type of subcolumn is Nullable
        /// and default values are NULLs.
        bool is_nullable = false;

        /// Parts of column. Parts should be in increasing order in terms of subtypes/supertypes.
        /// That means that the least common type for i-th prefix is the type of i-th part
        /// and it's the supertype for all type of column from 0 to i-1.
        std::vector<WrappedPtr> data;

        /// Until we insert any non-default field we don't know further
        /// least common type and we count number of defaults in prefix,
        /// which will be converted to the default type of final common type.
        size_t num_of_defaults_in_prefix = 0;

        size_t num_rows = 0;
    };

    using Subcolumns = SubcolumnsTree<Subcolumn>;

private:
    /// If true then all subcolumns are nullable.
    const bool is_nullable;

    Subcolumns subcolumns;
    size_t num_rows;

public:
    static constexpr auto COLUMN_NAME_DUMMY = "_dummy";

    explicit ColumnObjectDeprecated(bool is_nullable_);
    ColumnObjectDeprecated(Subcolumns && subcolumns_, bool is_nullable_);

    /// Checks that all subcolumns have consistent sizes.
    void checkConsistency() const;

    bool hasSubcolumn(const PathInData & key) const;

    const Subcolumn & getSubcolumn(const PathInData & key) const;
    Subcolumn & getSubcolumn(const PathInData & key);

    void incrementNumRows() { ++num_rows; }

    /// Adds a subcolumn from existing IColumn.
    void addSubcolumn(const PathInData & key, MutableColumnPtr && subcolumn);

    /// Adds a subcolumn of specific size with default values.
    void addSubcolumn(const PathInData & key, size_t new_size);

    /// Adds a subcolumn of type Nested of specific size with default values.
    /// It cares about consistency of sizes of Nested arrays.
    void addNestedSubcolumn(const PathInData & key, const FieldInfo & field_info, size_t new_size);

    /// Finds a subcolumn from the same Nested type as @entry and inserts
    /// an array with default values with consistent sizes as in Nested type.
    bool tryInsertDefaultFromNested(const Subcolumns::NodePtr & entry) const;
    bool tryInsertManyDefaultsFromNested(const Subcolumns::NodePtr & entry) const;

    const Subcolumns & getSubcolumns() const { return subcolumns; }
    Subcolumns & getSubcolumns() { return subcolumns; }
    PathsInData getKeys() const;

    /// Part of interface

    const char * getFamilyName() const override { return "Object"; }
    TypeIndex getDataType() const override { return TypeIndex::ObjectDeprecated; }

    size_t size() const override;
    size_t byteSize() const override;
    size_t allocatedBytes() const override;
    void forEachSubcolumn(MutableColumnCallback callback) override;
    void forEachSubcolumnRecursively(RecursiveMutableColumnCallback callback) override;
    void insert(const Field & field) override;
    bool tryInsert(const Field & field) override;
    void insertDefault() override;
#if !defined(DEBUG_OR_SANITIZER_BUILD)
    void insertFrom(const IColumn & src, size_t n) override;
    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;
#else
    void doInsertFrom(const IColumn & src, size_t n) override;
    void doInsertRangeFrom(const IColumn & src, size_t start, size_t length) override;
#endif
    void popBack(size_t length) override;
    Field operator[](size_t n) const override;
    void get(size_t n, Field & res) const override;

    ColumnPtr permute(const Permutation & perm, size_t limit) const override;
    ColumnPtr filter(const Filter & filter, ssize_t result_size_hint) const override;
    ColumnPtr index(const IColumn & indexes, size_t limit) const override;
    ColumnPtr replicate(const Offsets & offsets) const override;
    MutableColumnPtr cloneResized(size_t new_size) const override;

    /// Finalizes all subcolumns.
    void finalize() override;
    bool isFinalized() const override;

    /// Order of rows in ColumnObjectDeprecated is undefined.
    void getPermutation(PermutationSortDirection, PermutationSortStability, size_t, int, Permutation & res) const override;
    void updatePermutation(PermutationSortDirection, PermutationSortStability, size_t, int, Permutation &, EqualRanges &) const override {}
#if !defined(DEBUG_OR_SANITIZER_BUILD)
    int compareAt(size_t, size_t, const IColumn &, int) const override { return 0; }
#else
    int doCompareAt(size_t, size_t, const IColumn &, int) const override { return 0; }
#endif
    void getExtremes(Field & min, Field & max) const override;

    /// All other methods throw exception.

    StringRef getDataAt(size_t) const override { throwMustBeConcrete(); }
    bool isDefaultAt(size_t) const override { throwMustBeConcrete(); }
    void insertData(const char *, size_t) override { throwMustBeConcrete(); }
    StringRef serializeValueIntoArena(size_t, Arena &, char const *&) const override { throwMustBeConcrete(); }
    char * serializeValueIntoMemory(size_t, char *) const override { throwMustBeConcrete(); }
    const char * deserializeAndInsertFromArena(const char *) override { throwMustBeConcrete(); }
    const char * skipSerializedInArena(const char *) const override { throwMustBeConcrete(); }
    void updateHashWithValue(size_t, SipHash &) const override { throwMustBeConcrete(); }
    WeakHash32 getWeakHash32() const override { throwMustBeConcrete(); }
    void updateHashFast(SipHash &) const override;
    void expand(const Filter &, bool) override { throwMustBeConcrete(); }
    bool hasEqualValues() const override { throwMustBeConcrete(); }
    size_t byteSizeAt(size_t) const override { throwMustBeConcrete(); }
    double getRatioOfDefaultRows(double) const override { throwMustBeConcrete(); }
    UInt64 getNumberOfDefaultRows() const override { throwMustBeConcrete(); }
    void getIndicesOfNonDefaultRows(Offsets &, size_t, size_t) const override { throwMustBeConcrete(); }

private:
    [[noreturn]] static void throwMustBeConcrete()
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ColumnObjectDeprecated must be converted to ColumnTuple before use");
    }

    template <typename Func>
    MutableColumnPtr applyForSubcolumns(Func && func) const;

    /// It's used to get shared sized of Nested to insert correct default values.
    const Subcolumns::Node * getLeafOfTheSameNested(const Subcolumns::NodePtr & entry) const;
};
}
