#include <Columns/ColumnWithDictionary.h>
#include <DataStreams/ColumnGathererStream.h>

namespace DB
{

ColumnWithDictionary::ColumnWithDictionary(MutableColumnPtr && column_unique_, MutableColumnPtr && indexes_)
    : column_unique(std::move(column_unique_)), indexes(std::move(indexes_))
{
    if (!dynamic_cast<const IColumnUnique *>(column_unique.get()))
        throw Exception("ColumnUnique expected as argument of ColumnWithDictionary.", ErrorCodes::ILLEGAL_COLUMN);

    getSizeOfCurrentIndexType();
}

ColumnWithDictionary::ColumnWithDictionary(const ColumnWithDictionary & other)
        : column_unique(other.column_unique), indexes(other.indexes)
{
}

void ColumnWithDictionary::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(*this);
}

MutableColumnPtr ColumnWithDictionary::cloneResized(size_t size) const
{
    auto unique_ptr = column_unique;
    return ColumnWithDictionary::create((*std::move(unique_ptr)).mutate(), indexes->cloneResized(size));
}

size_t ColumnWithDictionary::getSizeOfCurrentIndexType() const
{
    if (typeid_cast<const ColumnUInt8 *>(indexes.get()))
        return sizeof(UInt8);
    if (typeid_cast<const ColumnUInt16 *>(indexes.get()))
        return sizeof(UInt16);
    if (typeid_cast<const ColumnUInt32 *>(indexes.get()))
        return sizeof(UInt32);
    if (typeid_cast<const ColumnUInt64 *>(indexes.get()))
        return sizeof(UInt64);

    throw Exception("Unexpected indexes type for ColumnWithDictionary. Expected ColumnUInt, got " + indexes->getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
}

template <typename IndexType>
void ColumnWithDictionary::convertIndexes()
{
    auto convert = [&](auto x)
    {
        using CurIndexType = typeof(x);
        if (auto * index_col = typeid_cast<const ColumnVector<CurIndexType> *>(indexes.get()))
        {
            if (sizeof(CurIndexType) != sizeof(IndexType))
            {
                size_t size = index_col->size();
                auto new_index_col = ColumnVector<IndexType>::create(size);
                auto & data = index_col->getData();
                auto & new_data = new_index_col->getData();

                for (size_t i = 0; i < size; ++i)
                    new_data[i] = data[i];

                indexes = std::move(new_index_col);
            }

            return true;
        }
        return false;
    };

    if (!convert(UInt8()) &&
        !convert(UInt16()) &&
        !convert(UInt32()) &&
        !convert(UInt64()))
        throw Exception("Unexpected indexes type for ColumnWithDictionary. Expected ColumnUInt, got "
                        + indexes->getName(), ErrorCodes::ILLEGAL_COLUMN);
}

void ColumnWithDictionary::insertIndex(size_t value)
{
    auto current_index_type = getSizeOfCurrentIndexType();

    auto insertForType = [&](auto x)
    {
        using IndexType = typeof(x);
        if (value <= std::numeric_limits<IndexType>::max())
        {
            if (sizeof(IndexType) > current_index_type)
                convertIndexes<IndexType>();

            getIndexes()->insert(UInt64(value));

            return true;
        }
        return false;
    };

    if (!insertForType(UInt8()) &&
        !insertForType(UInt16()) &&
        !insertForType(UInt32()) &&
        !insertForType(UInt64()))
        throw Exception("Unexpected indexes type for ColumnWithDictionary.", ErrorCodes::ILLEGAL_COLUMN);
}

void ColumnWithDictionary::insertIndexesRange(const ColumnPtr & column)
{

}

}
