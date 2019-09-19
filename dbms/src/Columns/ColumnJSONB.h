#pragma once

#include <Core/Types.h>
#include <unordered_map>
#include <Core/ColumnsWithTypeAndName.h>
#include <Columns/IColumn.h>
#include <Columns/JSONB/JSONStructAndDataColumn.h>
#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>
#include <Common/HashTable/HashMap.h>

namespace DB
{

class ColumnJSONB;
using ColumnJSONBStruct = JSONStructAndDataColumn;
using ColumnJSONBStructPtr = std::shared_ptr<ColumnJSONBStruct>;

class ColumnJSONB final : public COWHelper<IColumn, ColumnJSONB>
{
public:
    const char * getFamilyName() const override { return "JSONB"; }

    size_t size() const override { return mark_columns.empty() ? 0 : mark_columns[0]->size(); }

    Field operator[](size_t row_num) const override;

    StringRef getDataAt(size_t n) const override;

    void get(size_t rows, Field & res) const override;

    void insert(const Field & field) override;

    void insertRangeFrom(const IColumn & src, size_t offset, size_t limit) override;

    void insertData(const char *pos, size_t length) override;

    size_t byteSize() const override;

    size_t allocatedBytes() const override;

    void popBack(size_t n) override;

    void insertDefault() override;

    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override;

    const char * deserializeAndInsertFromArena(const char * pos) override;

    void updateHashWithValue(size_t n, SipHash & hash) const override;

    ColumnPtr filter(const Filter & filter, ssize_t result_size_hint) const override;

    ColumnPtr permute(const Permutation & perm, size_t limit) const override;

    ColumnPtr index(const IColumn & indexes, size_t limit) const override;

    int compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override;

    void getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const override;

    ColumnPtr replicate(const Offsets & offsets) const override;

    std::vector<MutableColumnPtr> scatter(ColumnIndex num_columns, const Selector & selector) const override;

    void gather(ColumnGathererStream & gatherer_stream) override;

    void getExtremes(Field & min, Field & max) const override;

    MutableColumnPtr cloneEmpty() const override;

    IColumn * createMarkColumn();

    IColumn * createDataColumn(const DataTypePtr & type);

private:
    ColumnJSONBStructPtr info;
    std::vector<WrappedPtr> mark_columns;
    std::vector<WrappedPtr> data_columns;

    IColumn * insertBulkRowsWithDefaultValue(IColumn * column, size_t to_size);

    void insertNewStructFrom(const ColumnJSONBStructPtr & source_struct, ColumnJSONBStructPtr & to_struct, size_t offset, size_t limit, size_t old_size);

public:
    ColumnJSONB();

    ColumnJSONBStructPtr getStruct() { return info; }

    ColumnJSONBStructPtr getStruct() const { return info; }

    std::vector<WrappedPtr> & getFields() { return data_columns; }

    const std::vector<WrappedPtr> & getFields() const { return data_columns; }

    std::vector<WrappedPtr> & getMarks() { return mark_columns; }

    const std::vector<WrappedPtr> & getMarks() const { return mark_columns; }

    void insertFrom(const IColumn &src, size_t row_num) override;
};

}
