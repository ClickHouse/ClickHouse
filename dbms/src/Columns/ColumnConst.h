#pragma once

#include <Core/Field.h>
#include <Common/Exception.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnsCommon.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int NOT_IMPLEMENTED;
}


/** ColumnConst contains another column with single element,
  *  but looks like a column with arbitary amount of same elements.
  */
class ColumnConst : public IColumn
{
private:
    ColumnPtr data;
    size_t s;

public:
    ColumnConst(ColumnPtr data, size_t s);

    bool isConst() const override
    {
        return true;
    }

    ColumnPtr convertToFullColumn() const;

    ColumnPtr convertToFullColumnIfConst() const override
    {
        return convertToFullColumn();
    }

    std::string getName() const override
    {
        return "ColumnConst(" + data->getName() + ")";
    }

    bool isNumeric() const override
    {
        return data->isNumeric();
    }

    bool isNumericNotNullable() const override
    {
        return data->isNumericNotNullable();
    }

    bool isNullable() const override
    {
        return false;
    }

    bool isNull() const override;

    bool isFixed() const override
    {
        return data->isFixed();
    }

    size_t sizeOfField() const override
    {
        return data->sizeOfField();
    }

    ColumnPtr cloneResized(size_t new_size) const override
    {
        return std::make_shared<ColumnConst>(data, new_size);
    }

    size_t size() const override
    {
        return s;
    }

    Field operator[](size_t n) const override
    {
        return (*data)[0];
    }

    void get(size_t n, Field & res) const override
    {
        data->get(0, res);
    }

    StringRef getDataAt(size_t n) const override
    {
        return data->getDataAt(0);
    }

    StringRef getDataAtWithTerminatingZero(size_t n) const override
    {
        return data->getDataAtWithTerminatingZero(0);
    }

    UInt64 get64(size_t n) const override
    {
        return data->get64(0);
    }

    UInt64 getUInt(size_t n) const override
    {
        return data->getUInt(0);
    }

    Int64 getInt(size_t n) const override
    {
        return data->getInt(0);
    }

    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override
    {
        s += length;
    }

    void insert(const Field & x) override
    {
        ++s;
    }

    void insertData(const char * pos, size_t length) override
    {
        ++s;
    }

    void insertFrom(const IColumn & src, size_t n) override
    {
        ++s;
    }

    void insertDefault() override
    {
        ++s;
    }

    void popBack(size_t n) override
    {
        s -= n;
    }

    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override
    {
        return data->serializeValueIntoArena(0, arena, begin);
    }

    const char * deserializeAndInsertFromArena(const char * pos) override
    {
        auto res = data->deserializeAndInsertFromArena(pos);
        data->popBack(1);
        ++s;
        return res;
    }

    void updateHashWithValue(size_t n, SipHash & hash) const override
    {
        data->updateHashWithValue(0, hash);
    }

    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override
    {
        if (s != filt.size())
            throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

        return std::make_shared<ColumnConst>(data, countBytesInFilter(filt));
    }

    ColumnPtr replicate(const Offsets_t & offsets) const override
    {
        if (s != offsets.size())
            throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

        size_t replicated_size = 0 == s ? 0 : offsets.back();
        return std::make_shared<ColumnConst>(data, replicated_size);
    }

    size_t byteSize() const override
    {
        return data->byteSize() + sizeof(s);
    }

    size_t allocatedBytes() const override
    {
        return data->allocatedBytes() + sizeof(s);
    }

    ColumnPtr permute(const Permutation & perm, size_t limit) const override
    {
        if (limit == 0)
            limit = s;
        else
            limit = std::min(s, limit);

        if (perm.size() < limit)
            throw Exception("Size of permutation is less than required.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

        return std::make_shared<ColumnConst>(data, limit);
    }

    int compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override
    {
        return data->compareAt(0, 0, *static_cast<const ColumnConst &>(rhs).data, nan_direction_hint);
    }

    void getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const override
    {
        res.resize(s);
        for (size_t i = 0; i < s; ++i)
            res[i] = i;
    }

    Columns scatter(ColumnIndex num_columns, const Selector & selector) const override
    {
        if (size() != selector.size())
            throw Exception("Size of selector doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

        std::vector<size_t> counts(num_columns);
        for (auto idx : selector)
            ++counts[idx];

        Columns res(num_columns);
        for (size_t i = 0; i < num_columns; ++i)
            res[i] = cloneResized(counts[i]);

        return res;
    }

    void gather(ColumnGathererStream &) override
    {
        throw Exception("Cannot gather into constant column " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    void getExtremes(Field & min, Field & max) const override
    {
        data->getExtremes(min, max);
    }


    /// Not part of the common interface.

    IColumn & getDataColumn() { return *data; }
    const IColumn & getDataColumn() const { return *data; }
    ColumnPtr & getDataColumnPtr() { return data; }
    const ColumnPtr & getDataColumnPtr() const { return data; }

    Field getField() const { return getDataColumn()[0]; }

    template <typename T>
    T getValue() const { return getField().safeGet<typename NearestFieldType<T>::Type>(); }

    /// Debug output.
    String dump() const;
};

}
