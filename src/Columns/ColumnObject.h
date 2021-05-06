#pragma once

#include <Core/Field.h>
#include <Core/Names.h>
#include <Columns/IColumn.h>
#include <Common/PODArray.h>
#include <Common/HashTable/HashMap.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class ColumnObject final : public COWHelper<IColumn, ColumnObject>
{
public:
    struct Subcolumn
    {
        Subcolumn() = default;
        Subcolumn(const Subcolumn & other);
        Subcolumn(MutableColumnPtr && data_);
        Subcolumn & operator=(Subcolumn && other) = default;

        WrappedPtr data;
        PaddedPODArray<TypeIndex> type_ids;

        size_t size() const { return data->size(); }
        void insert(const Field & field, TypeIndex type_id);
        void insertDefault();
        void resize(size_t new_size);
    };

    using SubcolumnsMap = std::unordered_map<String, Subcolumn>;

private:
    SubcolumnsMap subcolumns;

public:
    ColumnObject() = default;
    ColumnObject(SubcolumnsMap && subcolumns_);

    void checkConsistency() const;

    bool hasSubcolumn(const String & key) const;

    const Subcolumn & getSubcolumn(const String & key) const;
    Subcolumn & getSubcolumn(const String & key);

    void addSubcolumn(const String & key, MutableColumnPtr && column_sample, size_t new_size, bool check_size = false);
    void addSubcolumn(const String & key, Subcolumn && subcolumn, bool check_size = false);

    const SubcolumnsMap & getSubcolumns() const { return subcolumns; }
    SubcolumnsMap & getSubcolumns() { return subcolumns; }

    Names getKeys() const;

    /// Part of interface

    const char * getFamilyName() const override { return "Object"; }

    size_t size() const override { return subcolumns.empty() ? 0 : subcolumns.begin()->second.size(); }

    MutableColumnPtr cloneResized(size_t new_size) const override;

    size_t byteSize() const override;
    size_t allocatedBytes() const override;

    /// All other methods throw exception.

    ColumnPtr decompress() const override { throwMustBeDecompressed(); }

    TypeIndex getDataType() const override { throwMustBeDecompressed(); }
    Field operator[](size_t) const override { throwMustBeDecompressed(); }
    void get(size_t, Field &) const override { throwMustBeDecompressed(); }
    StringRef getDataAt(size_t) const override { throwMustBeDecompressed(); }
    void insert(const Field &) override { throwMustBeDecompressed(); }
    void insertRangeFrom(const IColumn &, size_t, size_t) override { throwMustBeDecompressed(); }
    void insertData(const char *, size_t) override { throwMustBeDecompressed(); }
    void insertDefault() override { throwMustBeDecompressed(); }
    void popBack(size_t) override { throwMustBeDecompressed(); }
    StringRef serializeValueIntoArena(size_t, Arena &, char const *&) const override { throwMustBeDecompressed(); }
    const char * deserializeAndInsertFromArena(const char *) override { throwMustBeDecompressed(); }
    const char * skipSerializedInArena(const char *) const override { throwMustBeDecompressed(); }
    void updateHashWithValue(size_t, SipHash &) const override { throwMustBeDecompressed(); }
    void updateWeakHash32(WeakHash32 &) const override { throwMustBeDecompressed(); }
    void updateHashFast(SipHash &) const override { throwMustBeDecompressed(); }
    ColumnPtr filter(const Filter &, ssize_t) const override { throwMustBeDecompressed(); }
    ColumnPtr permute(const Permutation &, size_t) const override { throwMustBeDecompressed(); }
    ColumnPtr index(const IColumn &, size_t) const override { throwMustBeDecompressed(); }
    int compareAt(size_t, size_t, const IColumn &, int) const override { throwMustBeDecompressed(); }
    void compareColumn(const IColumn &, size_t, PaddedPODArray<UInt64> *, PaddedPODArray<Int8> &, int, int) const override
    {
        throwMustBeDecompressed();
    }
    bool hasEqualValues() const override
    {
        throwMustBeDecompressed();
    }
    void getPermutation(bool, size_t, int, Permutation &) const override { throwMustBeDecompressed(); }
    void updatePermutation(bool, size_t, int, Permutation &, EqualRanges &) const override { throwMustBeDecompressed(); }
    ColumnPtr replicate(const Offsets &) const override { throwMustBeDecompressed(); }
    MutableColumns scatter(ColumnIndex, const Selector &) const override { throwMustBeDecompressed(); }
    void gather(ColumnGathererStream &) override { throwMustBeDecompressed(); }
    void getExtremes(Field &, Field &) const override { throwMustBeDecompressed(); }
    size_t byteSizeAt(size_t) const override { throwMustBeDecompressed(); }

private:
    [[noreturn]] void throwMustBeDecompressed() const
    {
        throw Exception("ColumnCompressed must be decompressed before use", ErrorCodes::LOGICAL_ERROR);
    }
};

}

