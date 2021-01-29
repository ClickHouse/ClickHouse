#include <Columns/ColumnMap.h>
#include <Columns/ColumnCompressed.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataStreams/ColumnGathererStream.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <ext/map.h>
#include <ext/range.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Common/WeakHash.h>
#include <Core/Field.h>
#include <common/sort.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

ColumnMap::ColumnMap(const ColumnMap & rhs)
: value_type(rhs.value_type)
, keysColumn(rhs.keysColumn->cloneResized(rhs.keysColumn->size()))
{
    for (const auto & elem : rhs.subColumns)
    {
        subColumns[elem.first] = elem.second->cloneResized(elem.second->size());
    }
}

ColumnMap::ColumnMap(DataTypePtr value_type_)
: value_type(value_type_)
{
}

ColumnMap::Ptr ColumnMap::create(DataTypePtr value_type, const std::vector<String> & keys, const Columns & subColumns)
{
    Ptr res = ColumnMap::create(value_type);
    MutableColumnPtr res_mut = res->assumeMutable();
    auto new_map = assert_cast<ColumnMap &>(*res_mut);
    DataTypePtr key_type = std::make_shared<DataTypeString>();
    MutableColumnPtr keysColumn = key_type->createColumn();
    size_t num_keys = keys.size();
    for (size_t i = 0; i < num_keys; ++i)
    {
        const String & key = keys[i];
        keysColumn->insert(key);
        new_map.subColumns[key] = subColumns[i];
    }
    new_map.keysColumn = std::move(keysColumn);
    return res;
}

ColumnMap::Ptr ColumnMap::create(DataTypePtr value_type, const ColumnPtr & col_keys, const ColumnPtr & col_values, const ColumnPtr & col_offsets)
{
    Ptr res = ColumnMap::create(value_type);
    MutableColumnPtr res_mut = res->assumeMutable();
    auto new_map = assert_cast<ColumnMap &>(*res_mut);
    const IColumn & conv_keys = *col_keys;
    const IColumn & conv_values = *col_values;

    const IColumn::Offsets & offsets = assert_cast<const ColumnArray::ColumnOffsets &>(*col_offsets).getData();
    for (size_t i_row = 0; i_row < offsets.size(); i_row++)
    {
        size_t orig_size = new_map.size();
        for (size_t i_elem = offsets[i_row-1]; i_elem < offsets[i_row]; i_elem++)
        {
            Field key_fld = conv_keys[i_elem];
            const String & key = key_fld.safeGet<String>();
            const auto & it = new_map.subColumns.find(key);
            if (it == new_map.subColumns.end())
            {
                new_map.keysColumn->assumeMutable()->insert(key_fld);
                MutableColumnPtr mcp = value_type->createColumn();
                mcp->insertManyDefaults(orig_size);
                mcp->insertFrom(conv_values, i_elem);
                new_map.subColumns[key] = std::move(mcp);
            }
            else if (it->second->size() == orig_size)
            {
                it->second->assumeMutable()->insertFrom(conv_values, i_elem);
            }
        }
        for (auto & elem : new_map.subColumns)
        {
            auto & column = elem.second;
            if (column->size() == orig_size)
            {
                column->assumeMutable()->insertDefault();
            }
        }
    }
    return res;
}

ColumnMap::Ptr ColumnMap::create(const ColumnPtr & column)
{
    const ColumnMap & src_map = assert_cast<const ColumnMap &>(*column);
    Ptr res = ColumnMap::create(src_map.value_type);
    MutableColumnPtr res_mut = res->assumeMutable();
    auto new_map = assert_cast<ColumnMap &>(*res_mut);
    new_map.keysColumn = src_map.keysColumn;
    for (const auto & elem : src_map.subColumns)
    {
        new_map.subColumns[elem.first] = elem.second;
    }
    return res;
}

std::string ColumnMap::getName() const
{
    WriteBufferFromOwnString res;
    res << "Map(";
    bool first_column(true);
    for (auto & elem : subColumns)
    {
        if (first_column)
        {
            first_column = false;
            res << ", ";
        }
        res << elem.second->getName();
    }
    res << ")";
    return res.str();
}

MutableColumnPtr ColumnMap::cloneEmpty() const
{
    return ColumnMap::create(value_type)->assumeMutable();
}

MutableColumnPtr ColumnMap::cloneResized(size_t new_size) const
{
    MutableColumnPtr res = cloneEmpty();
    auto new_map = assert_cast<ColumnMap &>(*res);
    new_map.keysColumn = keysColumn->cloneResized(keysColumn->size());
    for (auto & elem : subColumns)
    {
        new_map.subColumns[elem.first] = elem.second->cloneResized(new_size);
    }
    return res;
}

Field ColumnMap::operator[](size_t n) const
{
    Field fld;
    get(n, fld);
    return fld;
}

void ColumnMap::get(size_t n, Field & res) const
{
    Map mp;
    for (auto & elem : subColumns)
    {
        Field fld;
        elem.second->get(n, fld);
        mp[elem.first] = fld;
    }
    res = mp;
}

StringRef ColumnMap::getDataAt(size_t) const
{
    throw Exception("Method getDataAt is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

void ColumnMap::insertData(const char *, size_t)
{
    throw Exception("Method insertData is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

void ColumnMap::insert(const Field & x)
{
    const auto & map = DB::get<const Map &>(x);
    size_t orig_size = size();
    for (auto & elem : map)
    {
        const String & key = elem.first;
        const auto & it = subColumns.find(key);
        if (it == subColumns.end())
        {
            keysColumn->assumeMutable()->insert(key);
            MutableColumnPtr mcp = value_type->createColumn();
            mcp->insertManyDefaults(orig_size);
            mcp->insert(elem.second);
            subColumns[key] = std::move(mcp);
        }
        else
        {
            it->second->assumeMutable()->insert(elem.second);
        }
    }
    for (auto & elem : subColumns)
    {
        auto & column = elem.second;
        if (column->size() == orig_size)
        {
            column->assumeMutable()->insertDefault();
        }
    }
}

void ColumnMap::insertFrom(const IColumn & src, size_t n)
{
    insertRangeFrom(src, n, 1);
}

void ColumnMap::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    const ColumnMap & src_map = assert_cast<const ColumnMap &>(src);
    size_t orig_size = size();
    for (auto & src_elem : src_map.subColumns)
    {
        const String & key = src_elem.first;
        const auto & it = subColumns.find(key);
        if (it == subColumns.end())
        {
            keysColumn->assumeMutable()->insert(key);
            MutableColumnPtr mcp = src_elem.second->cloneEmpty();
            mcp->insertManyDefaults(orig_size);
            mcp->insertRangeFrom(*(src_elem.second), start, length);
            subColumns[key] = std::move(mcp);
        }
        else
        {
            it->second->assumeMutable()->insertRangeFrom(*(src_elem.second), start, length);
        }
    }
    for (auto & elem : subColumns)
    {
        if (elem.second->size() == orig_size)
        {
            elem.second->assumeMutable()->insertManyDefaults(length);
        }
    }
}

void ColumnMap::insertDefault()
{
    for (auto & elem : subColumns)
    {
        elem.second->assumeMutable()->insertDefault();
    }
}

void ColumnMap::popBack(size_t n)
{
    for (auto & elem : subColumns)
    {
        elem.second->assumeMutable()->popBack(n);
    }
}

StringRef ColumnMap::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
    size_t num_keys = subColumns.size();
    char * pos = arena.allocContinue(num_keys, begin);
    memcpy(pos, &num_keys, sizeof(num_keys));
    StringRef res(pos, sizeof(num_keys));

    for (auto & elem : subColumns)
    {
        const String & key = elem.first;
        size_t key_size = key.size();
        pos = arena.allocContinue(sizeof(key_size) + key_size, begin);
        memcpy(pos, &key_size, sizeof(key_size));
        memcpy(pos + sizeof(key_size), key.data(), key.size());
        res.data = pos - res.size;
        res.size += sizeof(key_size) + key_size;

        auto value_ref = elem.second->serializeValueIntoArena(n, arena, begin);
        res.data = value_ref.data - res.size;
        res.size += value_ref.size;
    }
    return res;
}

const char * ColumnMap::deserializeAndInsertFromArena(const char * pos)
{
    size_t orig_size = size();
    size_t num_keys = unalignedLoad<size_t>(pos);
    pos += sizeof(num_keys);
    for (size_t i = 0; i < num_keys; ++i)
    {
        const size_t string_size = unalignedLoad<size_t>(pos);
        String key(pos, string_size);
        pos += sizeof(string_size) + string_size;
        const auto & it = subColumns.find(key);
        if (it == subColumns.end())
        {
            keysColumn->assumeMutable()->insert(key);
            MutableColumnPtr mcp = value_type->createColumn();
            mcp->insertManyDefaults(orig_size);
            pos = mcp->deserializeAndInsertFromArena(pos);
            subColumns[key] = std::move(mcp);
        }
        else{
            pos = it->second->assumeMutable()->deserializeAndInsertFromArena(pos);
        }
    }
    for (auto & elem : subColumns)
    {
        if (elem.second->size() == orig_size)
        {
            elem.second->assumeMutable()->insertDefault();
        }
    }
    return pos;
}

const char * ColumnMap::skipSerializedInArena(const char * pos) const
{
    return nested->skipSerializedInArena(pos);
}

void ColumnMap::updateHashWithValue(size_t n, SipHash & hash) const
{
    for (auto & elem : subColumns)
    {
        elem.second->updateHashWithValue(n, hash);
    }
}

void ColumnMap::updateWeakHash32(WeakHash32 & hash) const
{
    for (auto & elem : subColumns)
    {
        elem.second->updateWeakHash32(hash);
    }
}

void ColumnMap::updateHashFast(SipHash & hash) const
{
    for (auto & elem : subColumns)
    {
        elem.second->updateHashFast(hash);
    }
}

ColumnPtr ColumnMap::filter(const Filter & filt, ssize_t result_size_hint) const
{
    std::vector<String> keys(subColumns.size());
    Columns new_sub_columns(subColumns.size());
    size_t i = 0;
    for (auto & elem : subColumns)
    {
        keys[i] = elem.first;
        new_sub_columns[i] = elem.second->filter(filt, result_size_hint);
        ++i;
    }
    return ColumnMap::create(value_type, keys, new_sub_columns);
}

ColumnPtr ColumnMap::permute(const Permutation & perm, size_t limit) const
{
    std::vector<String> keys(subColumns.size());
    Columns new_sub_columns(subColumns.size());
    size_t i = 0;
    for (auto & elem : subColumns)
    {
        keys[i] = elem.first;
        new_sub_columns[i] = elem.second->permute(perm, limit);
        ++i;
    }
    return ColumnMap::create(value_type, keys, new_sub_columns);
}

ColumnPtr ColumnMap::index(const IColumn & indexes, size_t limit) const
{
    std::vector<String> keys(subColumns.size());
    Columns new_sub_columns(subColumns.size());
    size_t i = 0;
    for (auto & elem : subColumns)
    {
        keys[i] = elem.first;
        new_sub_columns[i] = elem.second->index(indexes, limit);
        ++i;
    }
    return ColumnMap::create(value_type, keys, new_sub_columns);
}

ColumnPtr ColumnMap::replicate(const Offsets & offsets) const
{
    std::vector<String> keys(subColumns.size());
    Columns new_sub_columns(subColumns.size());
    size_t i = 0;
    for (auto & elem : subColumns)
    {
        keys[i] = elem.first;
        new_sub_columns[i] = elem.second->replicate(offsets);
        ++i;
    }
    return ColumnMap::create(value_type, keys, new_sub_columns);
}

int ColumnMap::compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const
{
    int res;
    if (this == &rhs)
    {
        for (auto & elem : subColumns)
        {
            const IColumn & column = *(elem.second);
            res = column.compareAt(n, m, column, nan_direction_hint);
            if (res != 0)
                return res;
        }
        return 0;
    }
    const auto & rhs_map = assert_cast<const ColumnMap &>(rhs);
    auto it_lhs = subColumns.begin();
    auto it_rhs = rhs_map.subColumns.begin();
    while (true)
    {
        if (it_lhs == subColumns.end())
        {
            return (it_rhs == rhs_map.subColumns.end()) ? 0: -1; 
        }
        else
        {
            if (it_rhs == rhs_map.subColumns.end())
                return 1;
            res = it_lhs->first.compare(it_rhs->first);
            if (res != 0)
                return res;
            res = it_lhs->second->compareAt(n, m, *(it_rhs->second), nan_direction_hint);
            if (res != 0)
                return res;
        }
        ++it_lhs;
        ++it_rhs;
    }
}

void ColumnMap::compareColumn(const IColumn & rhs, size_t rhs_row_num,
                                PaddedPODArray<UInt64> * row_indexes, PaddedPODArray<Int8> & compare_results,
                                int direction, int nan_direction_hint) const
{
    return doCompareColumn<ColumnMap>(assert_cast<const ColumnMap &>(rhs), rhs_row_num, row_indexes,
                                        compare_results, direction, nan_direction_hint);
}

bool ColumnMap::hasEqualValues() const
{
    return hasEqualValuesImpl<ColumnMap>();
}

void ColumnMap::getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const
{
    size_t rows = size();
    res.resize(rows);
    for (size_t i = 0; i < rows; ++i)
        res[i] = i;
    if (limit >= rows)
        limit = 0;

    const auto & less = [&](size_t a, size_t b) -> bool
    {
        int iret(0);
        for (const auto & elem : subColumns)
        {
            const IColumn & column = *(elem.second);
            iret = column.compareAt(a, b, column, nan_direction_hint);
            if (iret != 0)
                return (iret < 0) && !reverse;
        }
        return false;
    };

    if (limit)
        partial_sort(res.begin(), res.begin() + limit, res.end(), less);
    else
        std::sort(res.begin(), res.end(), less);
}

void ColumnMap::updatePermutation(bool reverse, size_t limit, int nan_direction_hint, IColumn::Permutation & res, EqualRanges & equal_ranges) const
{
    if (equal_ranges.empty())
        return;
    for (const auto & elem : subColumns)
    {
        elem.second->updatePermutation(reverse, limit, nan_direction_hint, res, equal_ranges);
        if (limit)
        {
            while (!equal_ranges.empty() && limit <= equal_ranges.back().first)
                equal_ranges.pop_back();
            if (equal_ranges.empty())
                break;
        }
    }
}

void ColumnMap::getExtremes(Field & min, Field & max) const
{
    size_t rows = size();
    if (rows == 0)
    {
        min = Map();
        max = Map();
        return;
    }
    Permutation res;
    res.resize(rows);
    for (size_t i = 0; i < rows; ++i)
        res[i] = i;

    bool reverse = false;
    const auto & less = [&](size_t a, size_t b) -> bool
    {
        int iret(0);
        for (const auto & elem : subColumns)
        {
            const IColumn & column = *(elem.second);
            iret = column.compareAt(a, b, column, -1);
            if (iret != 0)
                return (iret < 0) && !reverse;
        }
        return false;
    };

    partial_sort(res.begin(), res.begin() + 1, res.end(), less);
    get(0, min);
    reverse = true;
    partial_sort(res.begin(), res.begin() + 1, res.end(), less);
    get(0, max);
}

MutableColumns ColumnMap::scatter(ColumnIndex num_columns, const Selector & selector) const
{
    MutableColumns res;
    for (const auto & elem : subColumns)
    {
        auto scattered_columns = elem.second->scatter(num_columns, selector);
        if (res.empty())
        {
            for (size_t i = 0; i < scattered_columns.size(); ++i)
            {
                MutableColumnPtr mcp = cloneEmpty();
                res.push_back(std::move(mcp));
            }
        }
        for (size_t i = 0; i < scattered_columns.size(); ++i)
        {
            ColumnMap & colMap = assert_cast<ColumnMap &>(*res[i]);
            colMap.subColumns[elem.first] = std::move(scattered_columns[i]);
        }
    }
    return res;
}

void ColumnMap::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(*this);
}

void ColumnMap::reserve(size_t n)
{
    for (auto & elem : subColumns)
    {
        elem.second->assumeMutable()->reserve(n);
    }
}

size_t ColumnMap::byteSize() const
{
    size_t size = keysColumn->byteSize();
    for (auto & elem : subColumns)
    {
        size += elem.second->byteSize();
    }
    return size;
}

size_t ColumnMap::byteSizeAt(size_t n) const
{
    size_t size = 0;
    for (auto & elem : subColumns)
    {
        size += elem.second->byteSizeAt(n);
    }
    return size;
}

size_t ColumnMap::allocatedBytes() const
{
    size_t size = keysColumn->assumeMutable()->allocatedBytes();
    for (auto & elem : subColumns)
    {
        size += elem.second->assumeMutable()->allocatedBytes();
    }
    return size;
}

void ColumnMap::protect()
{
    keysColumn->assumeMutable()->protect();
    for (auto & elem : subColumns)
    {
        elem.second->assumeMutable()->protect();
    }
}

void ColumnMap::forEachSubcolumn(ColumnCallback callback)
{
    keysColumn->assumeMutable()->forEachSubcolumn(callback);
    for (auto & elem : subColumns)
    {
        elem.second->assumeMutable()->forEachSubcolumn(callback);
    }
}

bool ColumnMap::structureEquals(const IColumn & rhs) const
{
    if (const auto * rhs_map = typeid_cast<const ColumnMap *>(&rhs))
        return value_type->equals(*(rhs_map->value_type));
    return false;
}

ColumnPtr ColumnMap::getSubColumn(const String & key) const
{
    const auto & it = subColumns.find(key);
    if (it == subColumns.end())
        return nullptr;
    return it->second;
}

ColumnPtr ColumnMap::compress() const
{
    std::vector<String> keys;
    Columns sub_columns_compressed;
    size_t byte_size = 0;
    for (auto & elem : subColumns)
    {
        keys.push_back(elem.first);
        ColumnPtr data_compressed = elem.second->compress();
        sub_columns_compressed.push_back(data_compressed);
        byte_size += data_compressed->byteSize();
    }

    return ColumnCompressed::create(size(), byte_size,
        [value_type = this->value_type, keys = std::move(keys), sub_columns_compressed = std::move(sub_columns_compressed)]
        {
            Columns sub_columns_decompressed;
            for (auto & elem : sub_columns_compressed)
            {
                sub_columns_decompressed.push_back(elem->decompress());
            }
            return ColumnMap::create(value_type, keys, sub_columns_decompressed);
        });
}

}
