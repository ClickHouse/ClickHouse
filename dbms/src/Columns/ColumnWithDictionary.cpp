#include <Columns/ColumnWithDictionary.h>
#include <DataStreams/ColumnGathererStream.h>

namespace DB
{

ColumnWithDictionary::ColumnWithDictionary(MutableColumnPtr && column_unique_, MutableColumnPtr && indexes_)
    : column_unique(std::move(column_unique_)), indexes(std::move(indexes_))
{
    if (!dynamic_cast<const IColumnUnique *>(column_unique.get()))
        throw Exception("ColumnUnique expected as argument of ColumnWithDictionary.", ErrorCodes::ILLEGAL_COLUMN);
}

ColumnWithDictionary::ColumnWithDictionary(const ColumnWithDictionary & other)
        : column_unique(other.column_unique), indexes(other.indexes)
{
}

void ColumnWithDictionary::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(*this);
}

}
