#include <cassert>
#include <memory>
#include "Common/assert_cast.h"
#include "Columns/ColumnFSST.h"

#include "Columns/ColumnString.h"
#include "Core/Field.h"
#include "base/types.h"
#include "fsst.h"

namespace DB
{

Field ColumnFSST::compressField(const unsigned char * buffer, size_t size) const
{
    String result(buffer, buffer + size);
    if (is_compressed)
    {
        size_t compressed_len;
        const unsigned char * str_in[1] = {buffer};
        unsigned char * str_out[1];

        fsst_compress(fsst_encoder, 1, &size, str_in, 2 * size, reinterpret_cast<unsigned char *>(result.data()), &compressed_len, str_out);
    }

    return Field(result);
}

void ColumnFSST::decompressField(Field & x, size_t size) const
{
    if (!is_compressed)
    {
        auto string = x.dump();
        String uncompressed_string(size, ' ');
        fsst_decompress(
            &fsst_decoder,
            string.size(),
            reinterpret_cast<const unsigned char *>(string.data()),
            size,
            reinterpret_cast<unsigned char *>(uncompressed_string.data()));
        x = Field(uncompressed_string);
    }
}

Field ColumnFSST::operator[](size_t n) const
{
    String string(n, ' ');
    Field result(std::move(string));
    get(n, result);
    return result;
}

void ColumnFSST::get(size_t n, Field & res) const
{
    auto string = string_column->getDataAt(n);
    if (is_compressed)
    {
        fsst_decompress(
            &fsst_decoder,
            string.size,
            reinterpret_cast<const unsigned char *>(string.data),
            origin_lengths[n],
            reinterpret_cast<unsigned char *>(res.dump().data()));
    }
}

/* TODO later */
bool mustCompress()
{
    return false;
}

void ColumnFSST::compressColumn()
{
    if (!mustCompress() || is_compressed)
    {
        return;
    }

    /* create fsst */
    auto & casted_string_column = assert_cast<ColumnString &>(*string_column);
    size_t strings = string_column->size();
    std::unique_ptr<const unsigned char *[]> string_pointers(new const unsigned char *[strings]);
    bool zero_terminated = false; // not sure

    for (size_t ind = 0; ind < strings; ind++)
    {
        origin_lengths.push_back(casted_string_column.getOffsets()[ind + 1] - casted_string_column.getOffsets()[ind]);
        string_pointers[ind] = reinterpret_cast<const unsigned char *>(casted_string_column.getChars()[ind]);
    }

    fsst_encoder = fsst_create(strings, reinterpret_cast<size_t *>(origin_lengths.data()), string_pointers.get(), zero_terminated);

    /* update column state */
    fsst_export(fsst_encoder, reinterpret_cast<unsigned char *>(&fsst_decoder));
    is_compressed = true;
    offsets.resize_fill(strings, 0);

    /* compress string column */
    size_t size_out = casted_string_column.getOffsets().back() * 2 + 7;
    std::unique_ptr<unsigned char[]> buffer_out(new unsigned char[size_out]);
    std::unique_ptr<size_t[]> len_out(new size_t[size_out]);
    std::unique_ptr<unsigned char *[]> pointers_out(new unsigned char *[size_out]);

    size_t compressed_strings = fsst_compress(
        fsst_encoder,
        strings,
        reinterpret_cast<size_t *>(origin_lengths.data()),
        string_pointers.get(),
        size_out,
        buffer_out.get(),
        len_out.get(),
        pointers_out.get());
    assert(compressed_strings == strings);

    for (size_t ind = 0; ind < strings; ind++)
    {
        offsets.push_back(pointers_out[ind] - pointers_out[0]);
    }
}

void ColumnFSST::insert(const Field & x)
{
    insertData(x.dump().data(), x.dump().size());
}

bool ColumnFSST::tryInsert(const Field & x)
{
    insert(x);
    return true;
}

void ColumnFSST::insertData(const char * pos, size_t length)
{
    origin_lengths.push_back(length);

    if (!is_compressed)
    {
        string_column->insertData(pos, length);
        compressColumn();
        return;
    }

    auto compressed = compressField(reinterpret_cast<const unsigned char *>(pos), length);
    offsets.push_back(assert_cast<ColumnString &>(*string_column).getOffsets().back());
    string_column->insert(compressed);
}

void ColumnFSST::popBack(size_t n)
{
    string_column->popBack(n);
    while (n--)
    {
        offsets.pop_back();
        origin_lengths.pop_back();
    }
}

void ColumnFSST::getIndicesOfNonDefaultRows(Offsets & indices, size_t from, size_t limit) const
{
    for (size_t ind = from; ind < std::min(limit, string_column->size()); ind++)
    {
        indices[ind] = ind - from;
    }
}

size_t ColumnFSST::byteSize() const
{
    size_t body_size = sizeof(*this);
    size_t allocated_on_the_heap = (offsets.size() + origin_lengths.size()) * sizeof(offsets[0]) + string_column->byteSize();
    return body_size + allocated_on_the_heap;
}

size_t ColumnFSST::byteSizeAt(size_t n) const
{
    return string_column->byteSizeAt(n) + sizeof(offsets[0]);
}

size_t ColumnFSST::allocatedBytes() const
{
    return byteSize();
}

bool ColumnFSST::isDefaultAt(size_t n) const
{
    return string_column->isDefaultAt(n);
}

};
