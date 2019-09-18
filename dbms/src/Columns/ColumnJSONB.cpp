#include <utility>

#include <Columns/ColumnJSONB.h>
#include <DataStreams/ColumnGathererStream.h>
#include <Common/Arena.h>
#include <Common/SipHash.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <rapidjson/reader.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/JSONB/JSONBStreamBuffer.h>
#include <DataTypes/JSONB/JSONBSerialization.h>
#include <DataTypes/JSONB/JSONBStreamFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

Field ColumnJSONB::operator[](size_t row_num) const
{
    FormatSettings settings{};
    WriteBufferFromOwnString buffer;
    JSONBSerialization::serialize(*this, row_num,
        JSONBStreamFactory::fromBuffer<FormatStyle::ESCAPED>(static_cast<WriteBuffer *>(&buffer), settings));
    return Field(buffer.str());
}

void ColumnJSONB::get(size_t rows, Field & res) const
{
    res = operator[](rows);
}

void ColumnJSONB::insertDefault()
{
    /// Default value is TypeIndex::Nothing
    for (size_t index = 0; index < mark_columns.size(); ++index)
        mark_columns[index]->insertDefault();

    for (size_t index = 0; index < data_columns.size(); ++index)
        data_columns[index]->insertDefault();
}

void ColumnJSONB::insert(const Field & field)
{
    const String & s = DB::get<const String &>(field);
    insertData(s.data(), s.size());
}

void ColumnJSONB::insertData(const char * pos, size_t length)
{
    FormatSettings settings{};
    ReadBufferFromMemory buffer(pos, length);
    JSONBSerialization::deserialize(*this, JSONBStreamFactory::fromBuffer<FormatStyle::ESCAPED>(static_cast<ReadBuffer *>(&buffer), settings));
}

void ColumnJSONB::insertFrom(const IColumn & src, size_t row_num)
{
    insertRangeFrom(src, row_num, 1);
}

void ColumnJSONB::insertRangeFrom(const IColumn & src, size_t offset, size_t limit)
{
    limit = limit ? limit : src.size();
    const auto & source_column = static_cast<const ColumnJSONB &>(src);

    size_t old_size = size();
    size_t new_size = old_size + limit;
    insertNewStructFrom(source_column.info, info, offset, limit, old_size);

    for (size_t index = 0; index < mark_columns.size(); ++index)
        insertBulkRowsWithDefaultValue(mark_columns[index].get(), new_size);

    for (size_t index = 0; index < data_columns.size(); ++index)
        insertBulkRowsWithDefaultValue(data_columns[index].get(), new_size);
}

void ColumnJSONB::insertNewStructFrom(
    const ColumnJSONBStructPtr & source_struct, ColumnJSONBStructPtr & to_struct, size_t offset, size_t limit, size_t old_size)
{
    if (!source_struct->children.empty())
    {
        for (const auto & children : source_struct->children)
        {
            ColumnJSONBStructPtr children_struct = to_struct->getOrCreateChildren(children->name);
            insertNewStructFrom(children, children_struct, offset, limit, old_size);
        }

        if (!source_struct->mark_column && to_struct->mark_column)
        {
            auto to_marks_column = static_cast<ColumnUInt8 *>(to_struct->getOrCreateMarkColumn());
            ColumnUInt8::Container & to_marks_column_data = to_marks_column->getData();

            for (size_t index = 0; index < limit; ++index)
                to_marks_column_data.push_back(UInt8(TypeIndex::JSONB));
        }
    }

    if (source_struct->mark_column)
    {
        auto marks_column = to_struct->getOrCreateMarkColumn();
        insertBulkRowsWithDefaultValue(marks_column, old_size)->insertRangeFrom(*source_struct->mark_column, offset, limit);

        for (const auto & data_state : source_struct->data_columns)
        {
            auto data_column = to_struct->getOrCreateDataColumn(data_state.first);
            insertBulkRowsWithDefaultValue(data_column, old_size)->insertRangeFrom(*data_state.second, offset, limit);
        }
    }
}

void ColumnJSONB::popBack(size_t n)
{
    for (size_t index = 0; index < mark_columns.size(); ++index)
        mark_columns[index]->popBack(n);

    for (size_t index = 0; index < data_columns.size(); ++index)
        data_columns[index]->popBack(n);
}

size_t ColumnJSONB::byteSize() const
{
    size_t bytes_size = 0;

    for (size_t index = 0; index < mark_columns.size(); ++index)
        bytes_size += mark_columns[index]->byteSize();

    for (size_t index = 0; index < data_columns.size(); ++index)
        bytes_size += data_columns[index]->byteSize();

    return bytes_size;
}

size_t ColumnJSONB::allocatedBytes() const
{
    size_t allocated_size = 0;

    for (size_t index = 0; index < mark_columns.size(); ++index)
        allocated_size += mark_columns[index]->allocatedBytes();

    for (size_t index = 0; index < data_columns.size(); ++index)
        allocated_size += data_columns[index]->allocatedBytes();

    return allocated_size;
}

void ColumnJSONB::updateHashWithValue(size_t n, SipHash & hash) const
{
    for (size_t index = 0; index < mark_columns.size(); ++index)
        mark_columns[index]->updateHashWithValue(n, hash);

    for (size_t index = 0; index < data_columns.size(); ++index)
        data_columns[index]->updateHashWithValue(n, hash);
}

ColumnPtr ColumnJSONB::filter(const IColumn::Filter & filter, ssize_t result_size_hint) const
{
    ColumnJSONB::MutablePtr filtered_column = ColumnJSONB::create();

    filtered_column->data_columns.reserve(data_columns.size());
    filtered_column->mark_columns.reserve(mark_columns.size());
    filtered_column->info = info->clone(filtered_column.get(), [&](IColumn * need_filter_column, bool is_mark)
    {
        if (is_mark)
        {
            ColumnPtr filtered_mark_column = need_filter_column->filter(filter, result_size_hint);
            filtered_column->mark_columns.push_back(filtered_mark_column);
            return filtered_column->mark_columns.back().get();
        }
        else
        {
            ColumnPtr filtered_data_column = need_filter_column->filter(filter, result_size_hint);
            filtered_column->data_columns.push_back(filtered_data_column);
            return filtered_column->data_columns.back().get();
        }
    });

    return filtered_column;
}

ColumnPtr ColumnJSONB::permute(const IColumn::Permutation & perm, size_t limit) const
{
    ColumnJSONB::MutablePtr permuted_column = ColumnJSONB::create();

    permuted_column->data_columns.reserve(data_columns.size());
    permuted_column->mark_columns.reserve(mark_columns.size());
    permuted_column->info = info->clone(permuted_column.get(), [&](IColumn * need_permute_column, bool is_mark)
    {
        if (is_mark)
        {
            ColumnPtr permuted_mark_column = need_permute_column->permute(perm, limit);
            permuted_column->mark_columns.push_back(permuted_mark_column);
            return permuted_column->mark_columns.back().get();
        }
        else
        {
            ColumnPtr permuted_data_column = need_permute_column->permute(perm, limit);
            permuted_column->data_columns.push_back(permuted_data_column);
            return permuted_column->data_columns.back().get();
        }
    });

    return permuted_column;
}

ColumnPtr ColumnJSONB::index(const IColumn & indexes, size_t limit) const
{
    ColumnJSONB::MutablePtr indexed_column = ColumnJSONB::create();

    indexed_column->data_columns.reserve(data_columns.size());
    indexed_column->mark_columns.reserve(mark_columns.size());
    indexed_column->info = info->clone(indexed_column.get(), [&](IColumn * need_permute_column, bool is_mark)
    {
        if (is_mark)
        {
            ColumnPtr permuted_mark_column = need_permute_column->index(indexes, limit);
            indexed_column->mark_columns.push_back(permuted_mark_column);
            return indexed_column->mark_columns.back().get();
        }
        else
        {
            ColumnPtr permuted_data_column = need_permute_column->index(indexes, limit);
            indexed_column->data_columns.push_back(permuted_data_column);
            return indexed_column->data_columns.back().get();
        }
    });

    return indexed_column;
}

ColumnPtr ColumnJSONB::replicate(const IColumn::Offsets & offsets) const
{
    ColumnJSONB::MutablePtr replicated_column = ColumnJSONB::create();

    replicated_column->data_columns.reserve(data_columns.size());
    replicated_column->mark_columns.reserve(mark_columns.size());
    replicated_column->info = info->clone(replicated_column.get(), [&](IColumn * need_permute_column, bool is_mark)
    {
        if (is_mark)
        {
            ColumnPtr permuted_mark_column = need_permute_column->replicate(offsets);
            replicated_column->mark_columns.push_back(permuted_mark_column);
            return replicated_column->mark_columns.back().get();
        }
        else
        {
            ColumnPtr permuted_data_column = need_permute_column->replicate(offsets);
            replicated_column->data_columns.push_back(permuted_data_column);
            return replicated_column->data_columns.back().get();
        }
    });

    return replicated_column;
}

std::vector<MutableColumnPtr> ColumnJSONB::scatter(IColumn::ColumnIndex /*num_columns*/, const IColumn::Selector & /*selector*/) const
{
    throw Exception("", ErrorCodes::NOT_IMPLEMENTED);
//    ColumnJSONB::MutablePtr indexed_column = ColumnJSONB::create();
//
//    indexed_column->data_columns.reserve(data_columns.size());
//    indexed_column->mark_columns.reserve(mark_columns.size());
//    indexed_column->info = info->clone(indexed_column.get(), [&](IColumn * need_permute_column, bool is_mark)
//    {
//        if (is_mark)
//        {
//            ColumnPtr permuted_mark_column = need_permute_column->scatter(num_columns, selector);
//            indexed_column->mark_columns.push_back(permuted_mark_column);
//            return indexed_column->mark_columns.back().get();
//        }
//        else
//        {
//            ColumnPtr permuted_data_column = need_permute_column->scatter(num_columns, selector);
//            indexed_column->data_columns.push_back(permuted_data_column);
//            return indexed_column->data_columns.back().get();
//        }
//    });
//
//    return indexed_column;
}

void ColumnJSONB::gather(ColumnGathererStream & /*gatherer_stream*/)
{
    throw Exception("", ErrorCodes::NOT_IMPLEMENTED);
}

StringRef ColumnJSONB::getDataAt(size_t /*n*/) const
{
    throw Exception("Method getDataAt is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

void ColumnJSONB::getExtremes(Field & /*min*/, Field & /*max*/) const
{
    throw Exception("Method getExtremes is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

const char * ColumnJSONB::deserializeAndInsertFromArena(const char * /*pos*/)
{
    throw Exception("Method deserializeAndInsertFromArena is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

StringRef ColumnJSONB::serializeValueIntoArena(size_t /*n*/, Arena & /*arena*/, char const *& /*begin*/) const
{
    throw Exception("Method serializeValueIntoArena is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

int ColumnJSONB::compareAt(size_t /*n*/, size_t /*m*/, const IColumn & /*rhs*/, int /*nan_direction_hint*/) const
{
    throw Exception("Method compareAt is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

void ColumnJSONB::getPermutation(bool /*reverse*/, size_t /*limit*/, int /*nan_direction_hint*/, IColumn::Permutation & /*res*/) const
{
    throw Exception("Method getPermutation is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

IColumn * ColumnJSONB::insertBulkRowsWithDefaultValue(IColumn * column, size_t to_size)
{
    if (column->size() < to_size)
    {
        if (ColumnUInt8 * uint8_data_column = typeid_cast<ColumnUInt8 *>(column))
        {
            ColumnUInt8::Container & data_column_data = uint8_data_column->getData();

            data_column_data.reserve(to_size);
            for (size_t row = data_column_data.size(); row < to_size; ++row)
                data_column_data.push_back(UInt8());
        }
        else if (ColumnUInt64 * uint64_data_column = typeid_cast<ColumnUInt64 *>(column))
        {
            ColumnUInt64::Container & data_column_data = uint64_data_column->getData();

            data_column_data.reserve(to_size);
            for (size_t row = data_column_data.size(); row < to_size; ++row)
                data_column_data.push_back(UInt64());
        }
        else if (ColumnString * string_data_column = typeid_cast<ColumnString *>(column))
        {
            ColumnString::Chars & data_column_chars = string_data_column->getChars();
            ColumnString::Offsets & data_column_offsets = string_data_column->getOffsets();

            data_column_chars.reserve(to_size - data_column_offsets.size());
            data_column_offsets.reserve(to_size);

            for (size_t row = data_column_offsets.size(); row < to_size; ++row)
            {
                data_column_chars.push_back(UInt8(0));
                data_column_offsets.push_back(data_column_chars.size());
            }
        }
        else
            throw Exception("It is bug.", ErrorCodes::LOGICAL_ERROR);
    }

    return column;
}

ColumnJSONB::ColumnJSONB()
{
    info = std::make_shared<ColumnJSONBStruct>(this);
}

MutableColumnPtr ColumnJSONB::cloneEmpty() const
{
    return ColumnJSONB::create();
}

IColumn * ColumnJSONB::createMarkColumn()
{
    MutableColumnPtr mark_column = ColumnUInt8::create();
    mark_columns.push_back(std::move(mark_column));
    return mark_columns.back().get();
}

IColumn * ColumnJSONB::createDataColumn(const DataTypePtr & type)
{
    MutableColumnPtr data_column = type->createColumn();
    data_columns.push_back(std::move(data_column));
    return data_columns.back().get();
}

}
