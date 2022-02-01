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

struct FieldInfo
{
    DataTypePtr scalar_type;
    bool have_nulls;
    bool need_convert;
    size_t num_dimensions;
};

FieldInfo getFieldInfo(const Field & field);

class ColumnObject final : public COWHelper<IColumn, ColumnObject>
{
public:
    class Subcolumn
    {
    public:
        Subcolumn() = default;
        Subcolumn(size_t size_, bool is_nullable_);
        Subcolumn(MutableColumnPtr && data_, bool is_nullable_);

        size_t size() const;
        size_t byteSize() const;
        size_t allocatedBytes() const;

        bool isFinalized() const { return data.size() == 1 && num_of_defaults_in_prefix == 0; }
        const DataTypePtr & getLeastCommonType() const { return least_common_type; }
        void checkTypes() const;

        void insert(Field field);
        void insert(Field field, FieldInfo info);

        void insertDefault();
        void insertManyDefaults(size_t length);
        void insertRangeFrom(const Subcolumn & src, size_t start, size_t length);
        void popBack(size_t n);

        void finalize();

        Field getLastField() const;
        Subcolumn recreateWithDefaultValues(const FieldInfo & field_info) const;

        IColumn & getFinalizedColumn();
        const IColumn & getFinalizedColumn() const;
        const ColumnPtr & getFinalizedColumnPtr() const;

        friend class ColumnObject;

    private:
        DataTypePtr least_common_type;
        bool is_nullable = false;
        std::vector<WrappedPtr> data;
        size_t num_of_defaults_in_prefix = 0;
    };

    using SubcolumnsTree = SubcolumnsTree<Subcolumn>;

private:
    const bool is_nullable;

    SubcolumnsTree subcolumns;
    size_t num_rows;

public:
    static constexpr auto COLUMN_NAME_DUMMY = "_dummy";

    explicit ColumnObject(bool is_nullable_);
    ColumnObject(SubcolumnsTree && subcolumns_, bool is_nullable_);

    void checkConsistency() const;

    bool hasSubcolumn(const PathInData & key) const;

    const Subcolumn & getSubcolumn(const PathInData & key) const;
    Subcolumn & getSubcolumn(const PathInData & key);

    void incrementNumRows() { ++num_rows; }

    void addSubcolumn(const PathInData & key, MutableColumnPtr && subcolumn);
    void addSubcolumn(const PathInData & key, size_t new_size);
    void addNestedSubcolumn(const PathInData & key, const FieldInfo & field_info, size_t new_size);

    const SubcolumnsTree & getSubcolumns() const { return subcolumns; }
    SubcolumnsTree & getSubcolumns() { return subcolumns; }
    PathsInData getKeys() const;

    bool isFinalized() const;
    void finalize();

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
    void getPermutation(bool, size_t, int, Permutation &) const override { throwMustBeConcrete(); }
    void updatePermutation(bool, size_t, int, Permutation &, EqualRanges &) const override { throwMustBeConcrete(); }
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
