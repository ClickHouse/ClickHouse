#include <Columns/ColumnUnique.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

void throwNotImplementedForColumnUnique(const char * method)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method {} is not supported for ColumnUnique.", method);
}

template <typename ColumnType>
void ColumnUnique<ColumnType>::updateHashWithValue(size_t n, SipHash & hash_func) const
{
    return getNestedColumn()->updateHashWithValue(n, hash_func);
}

template <typename ColumnType>
UInt128 ColumnUnique<ColumnType>::IncrementalHash::getHash(const ColumnType & column)
{
    size_t column_size = column.size();
    UInt128 cur_hash;

    if (column_size != num_added_rows.load())
    {
        SipHash sip_hash;
        for (size_t i = 0; i < column_size; ++i)
            column.updateHashWithValue(i, sip_hash);

        std::lock_guard lock(mutex);
        hash = sip_hash.get128();
        cur_hash = hash;
        num_added_rows.store(column_size);
    }
    else
    {
        std::lock_guard lock(mutex);
        cur_hash = hash;
    }

    return cur_hash;
}

/// Explicit template instantiations.
template class ColumnUnique<ColumnInt8>;
template class ColumnUnique<ColumnUInt8>;
template class ColumnUnique<ColumnInt16>;
template class ColumnUnique<ColumnUInt16>;
template class ColumnUnique<ColumnInt32>;
template class ColumnUnique<ColumnUInt32>;
template class ColumnUnique<ColumnInt64>;
template class ColumnUnique<ColumnUInt64>;
template class ColumnUnique<ColumnInt128>;
template class ColumnUnique<ColumnUInt128>;
template class ColumnUnique<ColumnInt256>;
template class ColumnUnique<ColumnUInt256>;
template class ColumnUnique<ColumnBFloat16>;
template class ColumnUnique<ColumnFloat32>;
template class ColumnUnique<ColumnFloat64>;
template class ColumnUnique<ColumnString>;
template class ColumnUnique<ColumnFixedString>;
template class ColumnUnique<ColumnDateTime64>;
template class ColumnUnique<ColumnTime64>;
template class ColumnUnique<ColumnIPv4>;
template class ColumnUnique<ColumnIPv6>;
template class ColumnUnique<ColumnUUID>;
template class ColumnUnique<ColumnDecimal<Decimal32>>;
template class ColumnUnique<ColumnDecimal<Decimal64>>;
template class ColumnUnique<ColumnDecimal<Decimal128>>;
template class ColumnUnique<ColumnDecimal<Decimal256>>;

}
