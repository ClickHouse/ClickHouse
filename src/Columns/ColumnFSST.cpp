#include "Columns/ColumnFSST.h"

#include "Core/Field.h"
#include "fsst.h"

namespace DB
{

Field ColumnFSST::operator[](size_t n) const
{
    String string(n, ' ');
    Field result(std::move(string));
    get(n, result);
    return result;
}

void ColumnFSST::get(size_t n, Field & res) const {
    auto string = string_column->getDataAt(n);
    fsst_decompress(
        &fsst,
        string.size,
        reinterpret_cast<const unsigned char *>(string.data),
        origin_lengths[n],
        reinterpret_cast<unsigned char *>(res.dump().data()));
}

size_t ColumnFSST::byteSize() const
{
    return sizeof(fsst) + offsets.size() * sizeof(offsets[0]) + string_column->byteSize();
}

size_t ColumnFSST::byteSizeAt(size_t n) const
{
    return string_column->byteSizeAt(n) + sizeof(offsets[0]);
}

bool ColumnFSST::isDefaultAt(size_t n) const
{
    return string_column->isDefaultAt(n);
}

};
