#include <algorithm>
#include <cassert>
#include <memory>
#include <optional>
#include "Columns/ColumnFSST.h"

#include "Core/Field.h"
#include "base/types.h"
#include "fsst.h"

namespace DB
{
std::optional<size_t> ColumnFSST::batchByRow(size_t row) const
{
    if (decoders.empty() || decoders[0].batch_start_index > row)
    {
        return std::nullopt;
    }

    size_t batch_ind
        = --std::lower_bound(
              decoders.begin(), decoders.end(), row, [](const BatchDsc & dsc, size_t value) { return dsc.batch_start_index <= value; })
        - decoders.begin();

    return batch_ind;
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
    auto batch_ind = batchByRow(n);
    if (!batch_ind.has_value())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "access out of bound");
    }

    auto compressed_data = string_column->getDataAt(n);
    const auto & batch_dsc = decoders[batch_ind.value()];
    String uncompressed_string(origin_lengths[n], ' ');
    fsst_decompress(
        reinterpret_cast<::fsst_decoder_t*>(batch_dsc.decoder.get()),
        compressed_data.size(),
        reinterpret_cast<const unsigned char *>(compressed_data.data()),
        origin_lengths[n],
        reinterpret_cast<unsigned char *>(uncompressed_string.data()));
    res = std::move(uncompressed_string);
}

void ColumnFSST::appendNewBatch(const CompressedField & x, std::shared_ptr<fsst_decoder_t> decoder)
{
    decoders.emplace_back(BatchDsc{.decoder = decoder, .batch_start_index = string_column->size()});
    append(x);
}
void ColumnFSST::append(const CompressedField & x)
{
    string_column->insert(x.value);
    origin_lengths.push_back(x.uncompressed_size);
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

void ColumnFSST::insertData(const char *  /*pos*/, size_t  /*length*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "TODO");
}

void ColumnFSST::popBack(size_t  /*n*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "TODO");
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
    return string_column->byteSize() + origin_lengths.size() * sizeof(UInt64) + decoders.size() * sizeof(BatchDsc);
}

size_t ColumnFSST::byteSizeAt(size_t  /*n*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "TODO");
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
