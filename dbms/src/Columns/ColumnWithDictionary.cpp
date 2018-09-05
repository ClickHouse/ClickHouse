#include <Columns/ColumnWithDictionary.h>
#include <Columns/ColumnsNumber.h>
#include <DataStreams/ColumnGathererStream.h>
#include <DataTypes/NumberTraits.h>
#include <Common/HashTable/HashMap.h>

namespace DB
{

namespace
{
    template <typename T>
    PaddedPODArray<T> * getIndexesData(IColumn & indexes)
    {
        auto * column = typeid_cast<ColumnVector<T> *>(&indexes);
        if (column)
            return &column->getData();

        return nullptr;
    }

    template <typename T>
    MutableColumnPtr mapUniqueIndexImplRef(PaddedPODArray<T> & index)
    {
        PaddedPODArray<T> copy(index.cbegin(), index.cend());

        HashMap<T, T> hash_map;
        for (auto val : index)
            hash_map.insert({val, hash_map.size()});

        auto res_col = ColumnVector<T>::create();
        auto & data = res_col->getData();

        data.resize(hash_map.size());
        for (auto val : hash_map)
            data[val.second] = val.first;

        for (auto & ind : index)
            ind = hash_map[ind];

        for (size_t i = 0; i < index.size(); ++i)
            if (data[index[i]] != copy[i])
                throw Exception("Expected " + toString(data[index[i]]) + ", but got " + toString(copy[i]), ErrorCodes::LOGICAL_ERROR);

        return std::move(res_col);
    }

    template <typename T>
    MutableColumnPtr mapUniqueIndexImpl(PaddedPODArray<T> & index)
    {
        if (index.empty())
            return ColumnVector<T>::create();

        auto size = index.size();

        T max_val = index[0];
        for (size_t i = 1; i < size; ++i)
            max_val = std::max(max_val, index[i]);

        /// May happen when dictionary is shared.
        if (max_val > size)
            return mapUniqueIndexImplRef(index);

        auto map_size = UInt64(max_val) + 1;
        PaddedPODArray<T> map(map_size, 0);
        T zero_pos_value = index[0];
        index[0] = 0;
        T cur_pos = 0;
        for (size_t i = 1; i < size; ++i)
        {
            T val = index[i];
            if (val != zero_pos_value && map[val] == 0)
            {
                ++cur_pos;
                map[val] = cur_pos;
            }

            index[i] = map[val];
        }

        auto res_col = ColumnVector<T>::create(UInt64(cur_pos) + 1);
        auto & data = res_col->getData();
        data[0] = zero_pos_value;
        for (size_t i = 0; i < map_size; ++i)
        {
            auto val = map[i];
            if (val)
                data[val] = static_cast<T>(i);
        }

        return std::move(res_col);
    }

    /// Returns unique values of column. Write new index to column.
    MutableColumnPtr mapUniqueIndex(IColumn & column)
    {
        if (auto * data_uint8 = getIndexesData<UInt8>(column))
            return mapUniqueIndexImpl(*data_uint8);
        else if (auto * data_uint16 = getIndexesData<UInt16>(column))
            return mapUniqueIndexImpl(*data_uint16);
        else if (auto * data_uint32 = getIndexesData<UInt32>(column))
            return mapUniqueIndexImpl(*data_uint32);
        else if (auto * data_uint64 = getIndexesData<UInt64>(column))
            return mapUniqueIndexImpl(*data_uint64);
        else
            throw Exception("Indexes column for getUniqueIndex must be ColumnUInt, got" + column.getName(),
                            ErrorCodes::LOGICAL_ERROR);
    }
}


ColumnWithDictionary::ColumnWithDictionary(MutableColumnPtr && column_unique_, MutableColumnPtr && indexes_)
    : dictionary(std::move(column_unique_)), idx(std::move(indexes_))
{
    idx.check(getDictionary().size());
}

void ColumnWithDictionary::insert(const Field & x)
{
    compactIfSharedDictionary();
    idx.insertPosition(dictionary.getColumnUnique().uniqueInsert(x));
    idx.check(getDictionary().size());
}

void ColumnWithDictionary::insertDefault()
{
    idx.insertPosition(getDictionary().getDefaultValueIndex());
}

void ColumnWithDictionary::insertFrom(const IColumn & src, size_t n)
{
    auto * src_with_dict = typeid_cast<const ColumnWithDictionary *>(&src);

    if (!src_with_dict)
        throw Exception("Expected ColumnWithDictionary, got" + src.getName(), ErrorCodes::ILLEGAL_COLUMN);

    size_t position = src_with_dict->getIndexes().getUInt(n);

    if (&src_with_dict->getDictionary() == &getDictionary())
    {
        /// Dictionary is shared with src column. Insert only index.
        idx.insertPosition(position);
    }
    else
    {
        compactIfSharedDictionary();
        const auto & nested = *src_with_dict->getDictionary().getNestedColumn();
        idx.insertPosition(dictionary.getColumnUnique().uniqueInsertFrom(nested, position));
    }

    idx.check(getDictionary().size());
}

void ColumnWithDictionary::insertFromFullColumn(const IColumn & src, size_t n)
{
    compactIfSharedDictionary();
    idx.insertPosition(dictionary.getColumnUnique().uniqueInsertFrom(src, n));
    idx.check(getDictionary().size());
}

void ColumnWithDictionary::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    auto * src_with_dict = typeid_cast<const ColumnWithDictionary *>(&src);

    if (!src_with_dict)
        throw Exception("Expected ColumnWithDictionary, got" + src.getName(), ErrorCodes::ILLEGAL_COLUMN);

    if (&src_with_dict->getDictionary() == &getDictionary())
    {
        /// Dictionary is shared with src column. Insert only indexes.
        idx.insertPositionsRange(src_with_dict->getIndexes(), start, length);
    }
    else
    {
        compactIfSharedDictionary();

        /// TODO: Support native insertion from other unique column. It will help to avoid null map creation.

        auto sub_idx = (*src_with_dict->getIndexes().cut(start, length)).mutate();
        auto idx_map = mapUniqueIndex(*sub_idx);

        auto src_nested = src_with_dict->getDictionary().getNestedColumn();
        auto used_keys = src_nested->index(*idx_map, 0);

        auto inserted_indexes = dictionary.getColumnUnique().uniqueInsertRangeFrom(*used_keys, 0, used_keys->size());
        idx.insertPositionsRange(*inserted_indexes->index(*sub_idx, 0), 0, length);
    }
    idx.check(getDictionary().size());
}

void ColumnWithDictionary::insertRangeFromFullColumn(const IColumn & src, size_t start, size_t length)
{
    compactIfSharedDictionary();
    auto inserted_indexes = dictionary.getColumnUnique().uniqueInsertRangeFrom(src, start, length);
    idx.insertPositionsRange(*inserted_indexes, 0, length);
    idx.check(getDictionary().size());
}

void ColumnWithDictionary::insertRangeFromDictionaryEncodedColumn(const IColumn & keys, const IColumn & positions)
{
    Index(positions.getPtr()).check(keys.size());
    compactIfSharedDictionary();
    auto inserted_indexes = dictionary.getColumnUnique().uniqueInsertRangeFrom(keys, 0, keys.size());
    idx.insertPositionsRange(*inserted_indexes->index(positions, 0), 0, positions.size());
    idx.check(getDictionary().size());
}

void ColumnWithDictionary::insertData(const char * pos, size_t length)
{
    compactIfSharedDictionary();
    idx.insertPosition(dictionary.getColumnUnique().uniqueInsertData(pos, length));
    idx.check(getDictionary().size());
}

void ColumnWithDictionary::insertDataWithTerminatingZero(const char * pos, size_t length)
{
    compactIfSharedDictionary();
    idx.insertPosition(dictionary.getColumnUnique().uniqueInsertDataWithTerminatingZero(pos, length));
    idx.check(getDictionary().size());
}

StringRef ColumnWithDictionary::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
    return getDictionary().serializeValueIntoArena(getIndexes().getUInt(n), arena, begin);
}

const char * ColumnWithDictionary::deserializeAndInsertFromArena(const char * pos)
{
    compactIfSharedDictionary();

    const char * new_pos;
    idx.insertPosition(dictionary.getColumnUnique().uniqueDeserializeAndInsertFromArena(pos, new_pos));

    idx.check(getDictionary().size());
    return new_pos;
}

void ColumnWithDictionary::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(*this);
}

MutableColumnPtr ColumnWithDictionary::cloneResized(size_t size) const
{
    auto unique_ptr = dictionary.getColumnUniquePtr();
    return ColumnWithDictionary::create((*std::move(unique_ptr)).mutate(), getIndexes().cloneResized(size));
}

int ColumnWithDictionary::compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const
{
    const auto & column_with_dictionary = static_cast<const ColumnWithDictionary &>(rhs);
    size_t n_index = getIndexes().getUInt(n);
    size_t m_index = column_with_dictionary.getIndexes().getUInt(m);
    return getDictionary().compareAt(n_index, m_index, column_with_dictionary.getDictionary(), nan_direction_hint);
}

void ColumnWithDictionary::getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const
{
    if (limit == 0)
        limit = size();

    size_t unique_limit = std::min(limit, getDictionary().size());
    Permutation unique_perm;
    getDictionary().getNestedColumn()->getPermutation(reverse, unique_limit, nan_direction_hint, unique_perm);

    /// TODO: optimize with sse.

    /// Get indexes per row in column_unique.
    std::vector<std::vector<size_t>> indexes_per_row(getDictionary().size());
    size_t indexes_size = getIndexes().size();
    for (size_t row = 0; row < indexes_size; ++row)
        indexes_per_row[getIndexes().getUInt(row)].push_back(row);

    /// Replicate permutation.
    size_t perm_size = std::min(indexes_size, limit);
    res.resize(perm_size);
    size_t perm_index = 0;
    for (size_t row = 0; row < indexes_size && perm_index < perm_size; ++row)
    {
        const auto & row_indexes = indexes_per_row[unique_perm[row]];
        for (auto row_index : row_indexes)
        {
            res[perm_index] = row_index;
            ++perm_index;

            if (perm_index == perm_size)
                break;
        }
    }
}

std::vector<MutableColumnPtr> ColumnWithDictionary::scatter(ColumnIndex num_columns, const Selector & selector) const
{
    auto columns = getIndexes().scatter(num_columns, selector);
    for (auto & column : columns)
    {
        auto unique_ptr = dictionary.getColumnUniquePtr();
        column = ColumnWithDictionary::create((*std::move(unique_ptr)).mutate(), std::move(column));
    }

    return columns;
}

void ColumnWithDictionary::setSharedDictionary(const ColumnPtr & column_unique)
{
    if (!empty())
        throw Exception("Can't set ColumnUnique for ColumnWithDictionary because is't not empty.",
                        ErrorCodes::LOGICAL_ERROR);

    dictionary.setShared(column_unique);
}

ColumnWithDictionary::MutablePtr ColumnWithDictionary::compact()
{
    auto positions = idx.getPositions();
    /// Create column with new indexes and old dictionary.
    auto column = ColumnWithDictionary::create(getDictionary().assumeMutable(), (*std::move(positions)).mutate());
    /// Will create new dictionary.
    column->compactInplace();

    return column;
}

ColumnWithDictionary::MutablePtr ColumnWithDictionary::cutAndCompact(size_t start, size_t length) const
{
    auto sub_positions = (*idx.getPositions()->cut(start, length)).mutate();
    /// Create column with new indexes and old dictionary.
    auto column = ColumnWithDictionary::create(getDictionary().assumeMutable(), std::move(sub_positions));
    /// Will create new dictionary.
    column->compactInplace();

    return column;
}

void ColumnWithDictionary::compactInplace()
{
    auto positions = idx.detachPositions();
    dictionary.compact(positions);
    idx.attachPositions(std::move(positions));
}

void ColumnWithDictionary::compactIfSharedDictionary()
{
    if (dictionary.isShared())
        compactInplace();
}


ColumnWithDictionary::DictionaryEncodedColumn
ColumnWithDictionary::getMinimalDictionaryEncodedColumn(size_t offset, size_t limit) const
{
    MutableColumnPtr sub_indexes = (*std::move(idx.getPositions()->cut(offset, limit))).mutate();
    auto indexes_map = mapUniqueIndex(*sub_indexes);
    auto sub_keys = getDictionary().getNestedColumn()->index(*indexes_map, 0);

    return {std::move(sub_keys), std::move(sub_indexes)};
}


ColumnWithDictionary::Index::Index() : positions(ColumnUInt8::create()), size_of_type(sizeof(UInt8)) {}

ColumnWithDictionary::Index::Index(MutableColumnPtr && positions) : positions(std::move(positions))
{
    updateSizeOfType();
}

ColumnWithDictionary::Index::Index(ColumnPtr positions) : positions(std::move(positions))
{
    updateSizeOfType();
}

template <typename Callback>
void ColumnWithDictionary::Index::callForType(Callback && callback, size_t size_of_type)
{
    switch (size_of_type)
    {
        case sizeof(UInt8): { callback(UInt8()); break; }
        case sizeof(UInt16): { callback(UInt16()); break; }
        case sizeof(UInt32): { callback(UInt32()); break; }
        case sizeof(UInt64): { callback(UInt64()); break; }
        default: {
            throw Exception("Unexpected size of index type for ColumnWithDictionary: " + toString(size_of_type),
                            ErrorCodes::LOGICAL_ERROR);
        }
    }
}

size_t ColumnWithDictionary::Index::getSizeOfIndexType(const IColumn & column, size_t hint)
{
    auto checkFor = [&](auto type) { return typeid_cast<const ColumnVector<decltype(type)> *>(&column) != nullptr; };
    auto tryGetSizeFor = [&](auto type) -> size_t { return checkFor(type) ? sizeof(decltype(type)) : 0; };

    if (hint)
    {
        size_t size = 0;
        callForType([&](auto type) { size = tryGetSizeFor(type); }, hint);

        if (size)
            return size;
    }

    if (auto size = tryGetSizeFor(UInt8()))
        return size;
    if (auto size = tryGetSizeFor(UInt16()))
        return size;
    if (auto size = tryGetSizeFor(UInt32()))
        return size;
    if (auto size = tryGetSizeFor(UInt64()))
        return size;

    throw Exception("Unexpected indexes type for ColumnWithDictionary. Expected UInt, got " + column.getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
}

void ColumnWithDictionary::Index::attachPositions(ColumnPtr positions_)
{
    positions = std::move(positions_);
    updateSizeOfType();
}

template <typename IndexType>
typename ColumnVector<IndexType>::Container & ColumnWithDictionary::Index::getPositionsData()
{
    auto * positions_ptr = typeid_cast<ColumnVector<IndexType> *>(positions->assumeMutable().get());
    if (!positions_ptr)
        throw Exception("Invalid indexes type for ColumnWithDictionary."
                        " Expected UInt" + toString(8 * sizeof(IndexType)) + ", got " + positions->getName(),
                        ErrorCodes::LOGICAL_ERROR);

    return positions_ptr->getData();
}

template <typename IndexType>
void ColumnWithDictionary::Index::convertPositions()
{
    auto convert = [&](auto x)
    {
        using CurIndexType = decltype(x);
        auto & data = getPositionsData<CurIndexType>();

        if (sizeof(CurIndexType) > sizeof(IndexType))
            throw Exception("Converting indexes to smaller type: from " + toString(sizeof(CurIndexType)) +
                            " to " + toString(sizeof(IndexType)), ErrorCodes::LOGICAL_ERROR);

        if (sizeof(CurIndexType) != sizeof(IndexType))
        {
            size_t size = data.size();
            auto new_positions = ColumnVector<IndexType>::create(size);
            auto & new_data = new_positions->getData();

            /// TODO: Optimize with SSE?
            for (size_t i = 0; i < size; ++i)
                new_data[i] = data[i];

            positions = std::move(new_positions);
            size_of_type = sizeof(IndexType);
        }
    };

    callForType(std::move(convert), size_of_type);

    checkSizeOfType();
}

void ColumnWithDictionary::Index::expandType()
{
    auto expand = [&](auto type)
    {
        using CurIndexType = decltype(type);
        constexpr auto next_size = NumberTraits::nextSize(sizeof(CurIndexType));
        if (next_size == sizeof(CurIndexType))
            throw Exception("Can't expand indexes type for ColumnWithDictionary from type: "
                            + demangle(typeid(CurIndexType).name()), ErrorCodes::LOGICAL_ERROR);

        using NewIndexType = typename NumberTraits::Construct<false, false, next_size>::Type;
        convertPositions<NewIndexType>();
    };

    callForType(std::move(expand), size_of_type);
}

UInt64 ColumnWithDictionary::Index::getMaxPositionForCurrentType() const
{
    UInt64 value = 0;
    callForType([&](auto type) { value = std::numeric_limits<decltype(type)>::max(); }, size_of_type);
    return value;
}

void ColumnWithDictionary::Index::insertPosition(UInt64 position)
{
    while (position > getMaxPositionForCurrentType())
        expandType();

    positions->assumeMutableRef().insert(UInt64(position));
    checkSizeOfType();
}

void ColumnWithDictionary::Index::insertPositionsRange(const IColumn & column, size_t offset, size_t limit)
{
    auto insertForType = [&](auto type)
    {
        using ColumnType = decltype(type);
        const auto * column_ptr = typeid_cast<const ColumnVector<ColumnType> *>(&column);

        if (!column_ptr)
            return false;

        if (size_of_type < sizeof(ColumnType))
            convertPositions<ColumnType>();

        if (size_of_type == sizeof(ColumnType))
            positions->assumeMutableRef().insertRangeFrom(column, offset, limit);
        else
        {
            auto copy = [&](auto cur_type)
            {
                using CurIndexType = decltype(cur_type);
                auto & positions_data = getPositionsData<CurIndexType>();
                const auto & column_data = column_ptr->getData();

                size_t size = positions_data.size();
                positions_data.resize(size + limit);

                for (size_t i = 0; i < limit; ++i)
                    positions_data[size + i] = column_data[offset + i];
            };

            callForType(std::move(copy), size_of_type);
        }

        return true;
    };

    if (!insertForType(UInt8()) &&
        !insertForType(UInt16()) &&
        !insertForType(UInt32()) &&
        !insertForType(UInt64()))
        throw Exception("Invalid column for ColumnWithDictionary index. Expected UInt, got " + column.getName(),
                        ErrorCodes::ILLEGAL_COLUMN);

    checkSizeOfType();
}

void ColumnWithDictionary::Index::check(size_t /*max_dictionary_size*/)
{
    /// TODO: remove
    /*
    auto check = [&](auto cur_type)
    {
        using CurIndexType = decltype(cur_type);
        auto & positions_data = getPositionsData<CurIndexType>();

        for (size_t i = 0; i < positions_data.size(); ++i)
        {
            if (positions_data[i] >= max_dictionary_size)
            {
                throw Exception("Found index " + toString(positions_data[i]) + " at position " + toString(i)
                                + " which is grated or equal than dictionary size " + toString(max_dictionary_size),
                                ErrorCodes::LOGICAL_ERROR);
            }
        }
    };

    callForType(std::move(check), size_of_type);
     */
}

void ColumnWithDictionary::Index::checkSizeOfType()
{
    if (size_of_type != getSizeOfIndexType(*positions, size_of_type))
        throw Exception("Invalid size of type. Expected "  + toString(8 * size_of_type) +
                        ", but positions are " + positions->getName(), ErrorCodes::LOGICAL_ERROR);
}


ColumnWithDictionary::Dictionary::Dictionary(MutableColumnPtr && column_unique_)
    : column_unique(std::move(column_unique_))
{
    checkColumn(*column_unique);
}
ColumnWithDictionary::Dictionary::Dictionary(ColumnPtr column_unique_)
    : column_unique(std::move(column_unique_))
{
    checkColumn(*column_unique);
}

void ColumnWithDictionary::Dictionary::checkColumn(const IColumn & column)
{

    if (!dynamic_cast<const IColumnUnique *>(&column))
        throw Exception("ColumnUnique expected as an argument of ColumnWithDictionary.", ErrorCodes::ILLEGAL_COLUMN);
}

void ColumnWithDictionary::Dictionary::setShared(const ColumnPtr & dictionary)
{
    checkColumn(*dictionary);

    column_unique = dictionary;
    shared = true;
}

void ColumnWithDictionary::Dictionary::compact(ColumnPtr & positions)
{
    auto new_column_unique = column_unique->cloneEmpty();

    auto & unique = getColumnUnique();
    auto & new_unique = static_cast<IColumnUnique &>(*new_column_unique);

    auto indexes = mapUniqueIndex(positions->assumeMutableRef());
    auto sub_keys = unique.getNestedColumn()->index(*indexes, 0);
    auto new_indexes = new_unique.uniqueInsertRangeFrom(*sub_keys, 0, sub_keys->size());

    positions = (*new_indexes->index(*positions, 0)).mutate();
    column_unique = std::move(new_column_unique);

    shared = false;
}

}
