#pragma once

#include <Core/Block.h>

namespace DB
{

/** ColumnMap stores dynamic subcolumns.
 */
class ColumnMap final : public COWHelper<IColumn, ColumnMap>
{
private:
    friend class COWHelper<IColumn, ColumnMap>;
    friend class DataTypeMap;
    friend class FunctionCast;

    DataTypePtr value_type;
    std::map<String, ColumnPtr> subColumns;

    ColumnMap() = delete;
    ColumnMap(const ColumnMap &);
    ColumnMap(DataTypePtr value_type);

public:
    /** Create immutable column using immutable arguments. This arguments may be shared with other columns.
      * Use IColumn::mutate in order to make mutable column and mutate shared nested columns.
      */
    using Base = COWHelper<IColumn, ColumnMap>;

    static Ptr create(DataTypePtr value_type){ return Base::create(value_type); }
    static Ptr create(DataTypePtr value_type, const std::vector<String> & keys, const Columns & sub_columns);
    static Ptr create(DataTypePtr value_type, const ColumnPtr & keys, const ColumnPtr & values, const ColumnPtr & offsets);

    static Ptr create(const ColumnPtr & column);
    static Ptr create(ColumnPtr && arg) { return create(arg); }

    std::string getName() const override;
    const char * getFamilyName() const override { return "Map"; }
    TypeIndex getDataType() const override { return TypeIndex::Map; }

    MutableColumnPtr cloneEmpty() const override;
    MutableColumnPtr cloneResized(size_t size) const override;

    size_t size() const override { auto it = subColumns.cbegin(); return it==subColumns.cend() ? 0 : it->second->size(); }

    Field operator[](size_t n) const override;
    void get(size_t n, Field & res) const override;

    StringRef getDataAt(size_t n) const override;
    void insertData(const char * pos, size_t length) override;
    void insert(const Field & x) override;
    void insertFrom(const IColumn & src, size_t n) override;
    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;
    void insertDefault() override;
    void popBack(size_t n) override;
    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override;
    const char * deserializeAndInsertFromArena(const char * pos) override;
    const char * skipSerializedInArena(const char * pos) const override;
    void updateHashWithValue(size_t n, SipHash & hash) const override;
    void updateWeakHash32(WeakHash32 & hash) const override;
    void updateHashFast(SipHash & hash) const override;
    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override;
    ColumnPtr permute(const Permutation & perm, size_t limit) const override;
    ColumnPtr index(const IColumn & indexes, size_t limit) const override;
    ColumnPtr replicate(const Offsets & offsets) const override;
    MutableColumns scatter(ColumnIndex num_columns, const Selector & selector) const override;
    void gather(ColumnGathererStream & gatherer_stream) override;
    int compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override;
    void compareColumn(const IColumn & rhs, size_t rhs_row_num,
                       PaddedPODArray<UInt64> * row_indexes, PaddedPODArray<Int8> & compare_results,
                       int direction, int nan_direction_hint) const override;
    bool hasEqualValues() const override;
    void getExtremes(Field & min, Field & max) const override;
    void getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const override;
    void updatePermutation(bool reverse, size_t limit, int nan_direction_hint, IColumn::Permutation & res, EqualRanges & equal_ranges) const override;
    void reserve(size_t n) override;
    size_t byteSize() const override;
    size_t byteSizeAt(size_t n) const override;
    size_t allocatedBytes() const override;
    void protect() override;
    void forEachSubcolumn(ColumnCallback callback) override;
    bool structureEquals(const IColumn & rhs) const override;

    ColumnPtr getSubColumn(const String & key) const;

    ColumnPtr compress() const override;
};

}
