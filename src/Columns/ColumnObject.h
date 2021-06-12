#pragma once

#include <Core/Field.h>
#include <Core/Names.h>
#include <Columns/IColumn.h>
#include <Common/PODArray.h>
#include <Common/HashTable/HashMap.h>

#include <DataTypes/IDataType.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class ColumnObject final : public COWHelper<IColumn, ColumnObject>
{
public:
    class Subcolumn
    {
    public:
        Subcolumn() = default;
        Subcolumn(size_t size_);
        Subcolumn(MutableColumnPtr && data_);
        Subcolumn(const Subcolumn & other);
        Subcolumn & operator=(Subcolumn && other) = default;

        size_t size() const;
        size_t byteSize() const;
        size_t allocatedBytes() const;

        bool isFinalized() const { return data.size() == 1 && num_of_defaults_in_prefix == 0; }
        const DataTypePtr & getLeastCommonType() const { return least_common_type; }
        void checkTypes() const;

        void insert(Field && field);
        void insertDefault();
        void finalize();

        IColumn & getFinalizedColumn();
        const IColumn & getFinalizedColumn() const;
        const ColumnPtr & getFinalizedColumnPtr() const;

    private:
        DataTypePtr least_common_type;
        std::vector<WrappedPtr> data;
        size_t num_of_defaults_in_prefix = 0;
    };

    using SubcolumnsMap = std::unordered_map<String, Subcolumn>;

private:
    SubcolumnsMap subcolumns;

public:
    static constexpr auto COLUMN_NAME_DUMMY = "_dummy";

    ColumnObject() = default;
    ColumnObject(SubcolumnsMap && subcolumns_);

    void checkConsistency() const;

    bool hasSubcolumn(const String & key) const;

    const Subcolumn & getSubcolumn(const String & key) const;
    Subcolumn & getSubcolumn(const String & key);

    void addSubcolumn(const String & key, size_t new_size, bool check_size = false);
    void addSubcolumn(const String & key, Subcolumn && subcolumn, bool check_size = false);

    const SubcolumnsMap & getSubcolumns() const { return subcolumns; }
    SubcolumnsMap & getSubcolumns() { return subcolumns; }

    bool isFinalized() const;
    void finalize();

    /// Part of interface

    const char * getFamilyName() const override { return "Object"; }

    size_t size() const override;
    MutableColumnPtr cloneResized(size_t new_size) const override;
    size_t byteSize() const override;
    size_t allocatedBytes() const override;
    void forEachSubcolumn(ColumnCallback callback) override;

    /// All other methods throw exception.

    ColumnPtr decompress() const override { throwMustBeConcrete(); }
    TypeIndex getDataType() const override { throwMustBeConcrete(); }
    Field operator[](size_t) const override { throwMustBeConcrete(); }
    void get(size_t, Field &) const override { throwMustBeConcrete(); }
    StringRef getDataAt(size_t) const override { throwMustBeConcrete(); }
    bool isDefaultAt(size_t) const override { throwMustBeConcrete(); }
    void insert(const Field &) override { throwMustBeConcrete(); }
    void insertRangeFrom(const IColumn &, size_t, size_t) override { throwMustBeConcrete(); }
    void insertData(const char *, size_t) override { throwMustBeConcrete(); }
    void insertDefault() override { throwMustBeConcrete(); }
    void popBack(size_t) override { throwMustBeConcrete(); }
    StringRef serializeValueIntoArena(size_t, Arena &, char const *&) const override { throwMustBeConcrete(); }
    const char * deserializeAndInsertFromArena(const char *) override { throwMustBeConcrete(); }
    const char * skipSerializedInArena(const char *) const override { throwMustBeConcrete(); }
    void updateHashWithValue(size_t, SipHash &) const override { throwMustBeConcrete(); }
    void updateWeakHash32(WeakHash32 &) const override { throwMustBeConcrete(); }
    void updateHashFast(SipHash &) const override { throwMustBeConcrete(); }
    ColumnPtr filter(const Filter &, ssize_t) const override { throwMustBeConcrete(); }
    ColumnPtr permute(const Permutation &, size_t) const override { throwMustBeConcrete(); }
    ColumnPtr index(const IColumn &, size_t) const override { throwMustBeConcrete(); }
    int compareAt(size_t, size_t, const IColumn &, int) const override { throwMustBeConcrete(); }
    void compareColumn(const IColumn &, size_t, PaddedPODArray<UInt64> *, PaddedPODArray<Int8> &, int, int) const override { throwMustBeConcrete(); }
    bool hasEqualValues() const override { throwMustBeConcrete(); }
    void getPermutation(bool, size_t, int, Permutation &) const override { throwMustBeConcrete(); }
    void updatePermutation(bool, size_t, int, Permutation &, EqualRanges &) const override { throwMustBeConcrete(); }
    ColumnPtr replicate(const Offsets &) const override { throwMustBeConcrete(); }
    MutableColumns scatter(ColumnIndex, const Selector &) const override { throwMustBeConcrete(); }
    void gather(ColumnGathererStream &) override { throwMustBeConcrete(); }
    void getExtremes(Field &, Field &) const override { throwMustBeConcrete(); }
    size_t byteSizeAt(size_t) const override { throwMustBeConcrete(); }
    double getRatioOfDefaultRows(double) const override { throwMustBeConcrete(); }
    void getIndicesOfNonDefaultRows(Offsets &, size_t, size_t) const override { throwMustBeConcrete(); }

private:
    [[noreturn]] void throwMustBeConcrete() const
    {
        throw Exception("ColumnObject must be converted to ColumnTuple before use", ErrorCodes::LOGICAL_ERROR);
    }
};

}
