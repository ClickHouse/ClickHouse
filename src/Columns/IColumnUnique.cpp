#include <Columns/IColumnUnique.h>
#include <Common/SipHash.h>

namespace DB
{

UInt128 IColumnUnique::IncrementalHash::getHash(const ColumnPtr & column)
{
    size_t column_size = column->size();
    UInt128 cur_hash;

    if (column_size != num_added_rows.load())
    {
        SipHash sip_hash;
        for (size_t i = 0; i < column_size; ++i)
            column->updateHashWithValue(i, sip_hash);

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

};
