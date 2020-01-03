#pragma once

namespace DB {
/*
template<typename Out>
void BlockFile::getValue(size_t column, const PaddedPODArray<UInt64> & ids, ResultArrayType<Out> & out, PaddedPODArray<UInt64> & not_found) const
{
    std::vector<std::pair<size_t, size_t>> offsets;
    offsets.reserve(ids.size());

    for (size_t i = 0; i < ids.size(); ++i)
    {
        auto it = key_to_file_offset.find(ids[i]);
        if (it != std::end(key_to_file_offset))
        {
            offsets.emplace_back(it->second, i);
        }
        else
        {
            not_found.push_back(i);
        }
    }
    std::sort(std::begin(offsets), std::end(offsets));

    Field field;
    for (const auto & [offset, index] : offsets)
    {
        if (offset & OFFSET_MASK)
        {
            in_file.seek(offset && !OFFSET_MASK);
            for (size_t col = 0; col < column; ++col)
            {
                const auto & type = header.getByPosition(column).type;
                type->deserializeBinary(field, in_file);
            }
        }
        else
        {
            buffer[column]->get(offset, field);
        }
        out[index] = DB::get<Out>(field);
    }
}
*/
}