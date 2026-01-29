#include <Columns/ColumnIndex.h>
#include <DataTypes/NumberTraits.h>
#include <Common/WeakHash.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int LOGICAL_ERROR;
}


ColumnIndex::ColumnIndex() : indexes(ColumnUInt8::create()), size_of_type(sizeof(UInt8))
{
}

ColumnIndex::ColumnIndex(MutableColumnPtr && indexes_) : indexes(std::move(indexes_))
{
    updateSizeOfType();
}

ColumnIndex::ColumnIndex(ColumnPtr indexes_) : indexes(std::move(indexes_))
{
    updateSizeOfType();
}

template <typename Callback>
void ColumnIndex::callForType(Callback && callback, size_t size_of_type)
{
    switch (size_of_type)
    {
        case sizeof(UInt8): { callback(UInt8()); break; }
        case sizeof(UInt16): { callback(UInt16()); break; }
        case sizeof(UInt32): { callback(UInt32()); break; }
        case sizeof(UInt64): { callback(UInt64()); break; }
        default: {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected size of index type: {}",
                            size_of_type);
        }
    }
}

size_t ColumnIndex::getSizeOfIndexType(const IColumn & column, size_t hint)
{
    auto check_for = [&](auto type) { return typeid_cast<const ColumnVector<decltype(type)> *>(&column) != nullptr; };
    auto try_get_size_for = [&](auto type) -> size_t { return check_for(type) ? sizeof(decltype(type)) : 0; };

    if (hint)
    {
        size_t size = 0;
        callForType([&](auto type) { size = try_get_size_for(type); }, hint);

        if (size)
            return size;
    }

    if (auto size = try_get_size_for(UInt8()))
        return size;
    if (auto size = try_get_size_for(UInt16()))
        return size;
    if (auto size = try_get_size_for(UInt32()))
        return size;
    if (auto size = try_get_size_for(UInt64()))
        return size;

    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected indexes type for ColumnLowCardinality. Expected UInt, got {}",
                    column.getName());
}

void ColumnIndex::attachIndexes(MutableColumnPtr indexes_)
{
    indexes = std::move(indexes_);
    updateSizeOfType();
}

template <typename IndexType>
typename ColumnVector<IndexType>::Container & ColumnIndex::getIndexesData()
{
    auto * positions_ptr = typeid_cast<ColumnVector<IndexType> *>(indexes.get());
    if (!positions_ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid indexes type for ColumnLowCardinality. Expected UInt{}, got {}",
                        8 * sizeof(IndexType), indexes->getName());

    return positions_ptr->getData();
}

template <typename IndexType>
const typename ColumnVector<IndexType>::Container & ColumnIndex::getIndexesData() const
{
    const auto * positions_ptr = typeid_cast<const ColumnVector<IndexType> *>(indexes.get());
    if (!positions_ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid indexes type for ColumnLowCardinality. Expected UInt{}, got {}",
                        8 * sizeof(IndexType), indexes->getName());

    return positions_ptr->getData();
}

template <typename IndexType>
void ColumnIndex::convertIndexes()
{
    auto convert = [&](auto x)
    {
        using CurIndexType = decltype(x);
        auto & data = getIndexesData<CurIndexType>();

        if (sizeof(CurIndexType) > sizeof(IndexType))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Converting indexes to smaller type: from {} to {}",
                            sizeof(CurIndexType), sizeof(IndexType));

        if (sizeof(CurIndexType) != sizeof(IndexType))
        {
            size_t size = data.size();
            auto new_indexes = ColumnVector<IndexType>::create(size);
            auto & new_data = new_indexes->getData();

            /// TODO: Optimize with SSE?
            for (size_t i = 0; i < size; ++i)
                new_data[i] = static_cast<CurIndexType>(data[i]);

            indexes = std::move(new_indexes);
            size_of_type = sizeof(IndexType);
        }
    };

    callForType(std::move(convert), size_of_type);

    checkSizeOfType();
}

void ColumnIndex::expandType()
{
    auto expand = [&](auto type)
    {
        using CurIndexType = decltype(type);
        constexpr auto next_size = NumberTraits::nextSize(sizeof(CurIndexType));
        if (next_size == sizeof(CurIndexType))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't expand indexes type for ColumnLowCardinality from type: {}",
                            demangle(typeid(CurIndexType).name()));

        using NewIndexType = typename NumberTraits::Construct<false, false, next_size>::Type;
        convertIndexes<NewIndexType>();
    };

    callForType(std::move(expand), size_of_type);
}

size_t ColumnIndex::getMaxIndexForCurrentType() const
{
    size_t value = 0;
    callForType([&](auto type) { value = std::numeric_limits<decltype(type)>::max(); }, size_of_type);
    return value;
}

size_t ColumnIndex::getIndexAt(size_t row) const
{
    size_t index;
    auto get_index = [&](auto type)
    {
        using CurIndexType = decltype(type);
        index = getIndexesData<CurIndexType>()[row];
    };

    callForType(std::move(get_index), size_of_type);
    return index;
}


void ColumnIndex::insertIndex(size_t index)
{
    while (index > getMaxIndexForCurrentType())
        expandType();

    auto insert = [&](auto cur_type)
    {
        getIndexesData<decltype(cur_type)>().push_back(index);
    };

    callForType(std::move(insert), size_of_type);

    checkSizeOfType();
}

void ColumnIndex::insertManyIndexes(size_t index, size_t length)
{
    while (index > getMaxIndexForCurrentType())
        expandType();

    auto insert = [&](auto cur_type)
    {
        using CurIndexType = decltype(cur_type);
        auto & indexes_data = getIndexesData<CurIndexType>();
        indexes_data.resize_fill(indexes_data.size() + length, index);
    };

    callForType(std::move(insert), size_of_type);

    checkSizeOfType();
}

void ColumnIndex::insertIndexesRange(const IColumn & column, size_t offset, size_t limit)
{
    auto insert_for_type = [&](auto type)
    {
        using ColumnType = decltype(type);
        const auto * column_ptr = typeid_cast<const ColumnVector<ColumnType> *>(&column);

        if (!column_ptr)
            return false;

        if (size_of_type < sizeof(ColumnType))
            convertIndexes<ColumnType>();

        if (size_of_type == sizeof(ColumnType))
            indexes->insertRangeFrom(column, offset, limit);
        else
        {
            auto copy = [&](auto cur_type)
            {
                using CurIndexType = decltype(cur_type);
                auto & indexes_data = getIndexesData<CurIndexType>();
                const auto & column_data = column_ptr->getData();

                size_t size = indexes_data.size();
                indexes_data.resize(size + limit);

                for (size_t i = 0; i < limit; ++i)
                    indexes_data[size + i] = static_cast<CurIndexType>(column_data[offset + i]);
            };

            callForType(std::move(copy), size_of_type);
        }

        return true;
    };

    if (!insert_for_type(UInt8()) &&
        !insert_for_type(UInt16()) &&
        !insert_for_type(UInt32()) &&
        !insert_for_type(UInt64()))
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Invalid column for ColumnLowCardinality index. Expected UInt, got {}",
                        column.getName());

    checkSizeOfType();
}

void ColumnIndex::insertIndexesRangeWithShift(const IColumn & column, size_t offset, size_t limit, size_t shift, size_t max_result_index)
{
    while (max_result_index > getMaxIndexForCurrentType())
        expandType();

    auto insert_for_type = [&](auto type)
    {
        using ColumnType = decltype(type);
        const auto * column_ptr = typeid_cast<const ColumnVector<ColumnType> *>(&column);

        if (!column_ptr)
            return false;

        auto copy = [&](auto cur_type)
        {
            using CurIndexType = decltype(cur_type);
            auto & indexes_data = getIndexesData<CurIndexType>();
            const auto & column_data = column_ptr->getData();

            size_t size = indexes_data.size();
            indexes_data.resize(size + limit);

            for (size_t i = 0; i < limit; ++i)
                indexes_data[size + i] = static_cast<CurIndexType>(column_data[offset + i]) + shift;
        };

        callForType(std::move(copy), size_of_type);

        return true;
    };

    if (!insert_for_type(UInt8()) &&
        !insert_for_type(UInt16()) &&
        !insert_for_type(UInt32()) &&
        !insert_for_type(UInt64()))
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Invalid column for ColumnLowCardinality index. Expected UInt, got {}",
                        column.getName());

    checkSizeOfType();
}

void ColumnIndex::callForIndexes(std::function<void(size_t, size_t)> && callback, size_t start, size_t end) const
{
    auto callback_for_type = [&](auto cur_type)
    {
        using CurIndexType = decltype(cur_type);
        const auto & data = getIndexesData<CurIndexType>();
        for (size_t i = start; i != end; ++i)
            callback(i, data[i]);
    };

    callForType(std::move(callback_for_type), size_of_type);
}

ColumnPtr ColumnIndex::removeUnusedRowsInIndexedData(const ColumnPtr & indexed_data)
{
    /// First, create a filter for indexed data to filter out all unused rows.
    IColumn::Filter filter(indexed_data->size(), 0);
    auto create_filter = [&](auto cur_type)
    {
        using CurIndexType = decltype(cur_type);
        const auto & data = getIndexesData<CurIndexType>();
        for (size_t i = 0; i != data.size(); ++i)
            filter[data[i]] = 1;
    };

    callForType(std::move(create_filter), size_of_type);

    /// Second, adjust indexes.
    size_t result_size_hint = 0;
    auto adjust_indexes = [&](auto cur_type)
    {
        using CurIndexType = decltype(cur_type);
        PaddedPODArray<CurIndexType> indexes_remapping(indexed_data->size());
        size_t new_index = 0;
        for (size_t i = 0; i != filter.size(); ++i)
        {
            if (filter[i])
                indexes_remapping[i] = new_index++;
        }
        auto & data = getIndexesData<CurIndexType>();
        for (size_t i = 0; i != data.size(); ++i)
            data[i] = indexes_remapping[data[i]];

        result_size_hint = new_index;
    };

    callForType(std::move(adjust_indexes), size_of_type);
    return indexed_data->filter(filter, result_size_hint);
}

void ColumnIndex::getIndexesByMask(IColumn::Offsets & result_indexes, const PaddedPODArray<UInt8> & mask, size_t start, size_t end) const
{
    auto create_filter = [&](auto cur_type)
    {
        using CurIndexType = decltype(cur_type);
        const auto & data = getIndexesData<CurIndexType>();
        for (size_t i = start; i != end; ++i)
        {
            if (mask[data[i]])
                result_indexes.push_back(i);
        }
    };

    callForType(std::move(create_filter), size_of_type);
}


void ColumnIndex::insertIndexesRange(size_t start, size_t length)
{
    size_t max_index = start + length - 1;
    while (max_index > getMaxIndexForCurrentType())
        expandType();

    auto insert = [&](auto cur_type)
    {
        using CurIndexType = decltype(cur_type);
        auto & indexes_data = getIndexesData<CurIndexType>();
        indexes_data.reserve(indexes_data.size() + length);
        size_t end = start + length;
        for (size_t index = start; index != end; ++index)
            indexes_data.push_back(index);
    };

    callForType(std::move(insert), size_of_type);

    checkSizeOfType();
}

void ColumnIndex::resizeAssumeReserve(size_t n)
{
    auto resize = [&](auto cur_type)
    {
        using CurIndexType = decltype(cur_type);
        auto & indexes_data = getIndexesData<CurIndexType>();
        indexes_data.resize_assume_reserved(n);
    };

    callForType(std::move(resize), size_of_type);
}

void ColumnIndex::checkSizeOfType()
{
    if (size_of_type != getSizeOfIndexType(*indexes, size_of_type))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid size of type. Expected {}, but indexes are {}",
                        8 * size_of_type, indexes->getName());
}

void ColumnIndex::countRowsInIndexedData(ColumnUInt64::Container & counts) const
{
    auto counter = [&](auto x)
    {
        using CurIndexType = decltype(x);
        auto & data = getIndexesData<CurIndexType>();
        for (auto pos : data)
            ++counts[pos];
    };
    callForType(std::move(counter), size_of_type);
}

bool ColumnIndex::containsDefault() const
{
    bool contains = false;

    auto check_contains_default = [&](auto x)
    {
        using CurIndexType = decltype(x);
        auto & data = getIndexesData<CurIndexType>();
        for (auto pos : data)
        {
            if (pos == 0)
            {
                contains = true;
                break;
            }
        }
    };

    callForType(std::move(check_contains_default), size_of_type);
    return contains;
}

WeakHash32 ColumnIndex::getWeakHash(const WeakHash32 & dict_hash) const
{
    WeakHash32 hash(indexes->size());
    auto & hash_data = hash.getData();
    const auto & dict_hash_data = dict_hash.getData();

    auto update_weak_hash = [&](auto x)
    {
        using CurIndexType = decltype(x);
        auto & data = getIndexesData<CurIndexType>();
        auto size = data.size();

        for (size_t i = 0; i < size; ++i)
            hash_data[i] = dict_hash_data[data[i]];
    };

    callForType(std::move(update_weak_hash), size_of_type);
    return hash;
}

void ColumnIndex::collectSerializedValueSizes(
    PaddedPODArray<UInt64> & sizes, const PaddedPODArray<UInt64> & dict_sizes) const
{
    auto func = [&](auto x)
    {
        using CurIndexType = decltype(x);
        auto & data = getIndexesData<CurIndexType>();

        size_t rows = sizes.size();
        for (size_t i = 0; i < rows; ++i)
            sizes[i] += dict_sizes[data[i]];
    };
    callForType(std::move(func), size_of_type);
}

void ColumnIndex::expand(const IColumn::Filter & mask, bool inverted)
{
    indexes->expand(mask, inverted);
}

}
