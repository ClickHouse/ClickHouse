#pragma once

#include <Core/Field.h>
#include <Core/Names.h>
#include <Columns/IColumn.h>
#include <Common/PODArray.h>
#include <Common/HashTable/HashMap.h>
#include <DataTypes/Serializations/JSONDataParser.h>
#include <DataTypes/Serializations/SubcolumnsTree.h>

#include <DataTypes/IDataType.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
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
};

FieldInfo getFieldInfo(const Field & field);

/** A column that represents object with dynamic set of subcolumns.
 *  Subcolumns are identified by paths in document and are stored in
 *  a trie-like structure. ColumnObject is not suitable for writing into tables
 *  and it should be converted to Tuple with fixed set of subcolumns before that.
 */
class ColumnObject final : public COWHelper<IColumn, ColumnObject>
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

        bool isFinalized() const;
        const DataTypePtr & getLeastCommonType() const { return least_common_type.get(); }

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

        /// Converts all column's parts to the common type and
        /// creates a single column that stores all values.
        void finalize();

        /// Returns last inserted field.
        Field getLastField() const;

        /// Recreates subcolumn with default scalar values and keeps sizes of arrays.
        /// Used to create columns of type Nested with consistent array sizes.
        Subcolumn recreateWithDefaultValues(const FieldInfo & field_info) const;

        /// Returns single column if subcolumn in finalizes.
        /// Otherwise -- undefined behaviour.
        IColumn & getFinalizedColumn();
        const IColumn & getFinalizedColumn() const;
        const ColumnPtr & getFinalizedColumnPtr() const;

        friend class ColumnObject;

    private:
        class LeastCommonType
        {
        public:
            LeastCommonType() = default;
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
    };

    using SubcolumnsTree = SubcolumnsTree<Subcolumn>;

private:
    /// If true then all subcolumns are nullable.
    const bool is_nullable;

    SubcolumnsTree subcolumns;
    size_t num_rows;

public:
    static constexpr auto COLUMN_NAME_DUMMY = "_dummy";

    explicit ColumnObject(bool is_nullable_);
    ColumnObject(SubcolumnsTree && subcolumns_, bool is_nullable_);

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

    const SubcolumnsTree & getSubcolumns() const { return subcolumns; }
    SubcolumnsTree & getSubcolumns() { return subcolumns; }
    PathsInData getKeys() const;

    /// Finalizes all subcolumns.
    void finalize();
    bool isFinalized() const;

    /// Part of interface

    const char * getFamilyName() const override { return "Object"; }
    TypeIndex getDataType() const override { return TypeIndex::Object; }

    size_t size() const override;
    MutableColumnPtr cloneResized(size_t new_size) const override;
    size_t byteSize() const override;
    size_t allocatedBytes() const override;
    void forEachSubcolumn(ColumnCallback callback) override;
    void insert(const Field & field) override;
    void insertDefault() override;
    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;
    ColumnPtr replicate(const Offsets & offsets) const override;
    void popBack(size_t length) override;
    Field operator[](size_t n) const override;
    void get(size_t n, Field & res) const override;

    /// All other methods throw exception.

    ColumnPtr decompress() const override { throwMustBeConcrete(); }
    StringRef getDataAt(size_t) const override { throwMustBeConcrete(); }
    bool isDefaultAt(size_t) const override { throwMustBeConcrete(); }
    void insertData(const char *, size_t) override { throwMustBeConcrete(); }
    StringRef serializeValueIntoArena(size_t, Arena &, char const *&) const override { throwMustBeConcrete(); }
    const char * deserializeAndInsertFromArena(const char *) override { throwMustBeConcrete(); }
    const char * skipSerializedInArena(const char *) const override { throwMustBeConcrete(); }
    void updateHashWithValue(size_t, SipHash &) const override { throwMustBeConcrete(); }
    void updateWeakHash32(WeakHash32 &) const override { throwMustBeConcrete(); }
    void updateHashFast(SipHash &) const override { throwMustBeConcrete(); }
    ColumnPtr filter(const Filter &, ssize_t) const override { throwMustBeConcrete(); }
    void expand(const Filter &, bool) override { throwMustBeConcrete(); }
    ColumnPtr permute(const Permutation &, size_t) const override { throwMustBeConcrete(); }
    ColumnPtr index(const IColumn &, size_t) const override { throwMustBeConcrete(); }
    int compareAt(size_t, size_t, const IColumn &, int) const override { throwMustBeConcrete(); }
    void compareColumn(const IColumn &, size_t, PaddedPODArray<UInt64> *, PaddedPODArray<Int8> &, int, int) const override { throwMustBeConcrete(); }
    bool hasEqualValues() const override { throwMustBeConcrete(); }
    void getPermutation(PermutationSortDirection, PermutationSortStability, size_t, int, Permutation &) const override { throwMustBeConcrete(); }
    void updatePermutation(PermutationSortDirection, PermutationSortStability, size_t, int, Permutation &, EqualRanges &) const override { throwMustBeConcrete(); }
    MutableColumns scatter(ColumnIndex, const Selector &) const override { throwMustBeConcrete(); }
    void gather(ColumnGathererStream &) override { throwMustBeConcrete(); }
    void getExtremes(Field &, Field &) const override { throwMustBeConcrete(); }
    size_t byteSizeAt(size_t) const override { throwMustBeConcrete(); }
    double getRatioOfDefaultRows(double) const override { throwMustBeConcrete(); }
    void getIndicesOfNonDefaultRows(Offsets &, size_t, size_t) const override { throwMustBeConcrete(); }

private:
    [[noreturn]] static void throwMustBeConcrete()
    {
        throw Exception("ColumnObject must be converted to ColumnTuple before use", ErrorCodes::LOGICAL_ERROR);
    }
};

}
