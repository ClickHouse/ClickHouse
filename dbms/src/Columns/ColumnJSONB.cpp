#include <utility>

#include <Columns/ColumnJSONB.h>
#include <DataStreams/ColumnGathererStream.h>
#include <Common/Arena.h>
#include <Common/SipHash.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <rapidjson/reader.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/JSONBStreamBuffer.h>
#include <DataTypes/JSONBSerialization.h>
#include <DataTypes/JSONBStreamFactory.h>
#include <Columns/ColumnArray.h>
#include <Columns/JSONBDataMark.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace
{
using UniqueValueAndPos = std::tuple<ColumnPtr, ColumnPtr>;

size_t getMaxValue(const ColumnUInt64::Container & values)
{
    UInt64 max_value = values[0];
    for (size_t i = 1; i < values.size(); ++i)
        max_value = std::max(max_value, values[i]);
    return max_value;
}

template<size_t skip_number>
UniqueValueAndPos collectUniqueValueAndMarkPos(const ColumnUInt64::Container & values, size_t offset, size_t limit)
{
    if (values.empty())
        return {ColumnUInt64::create(), ColumnUInt64::create()};

    size_t cur_pos = 0;
    auto positions = ColumnUInt64::create(limit);
    auto & positions_data = positions->getData();
    ColumnUInt64::Container value_map_pos(getMaxValue(values) + 1, 0);

    for (size_t index = 0; index < limit; ++index)
    {
        auto value = values[offset + index];
        if (value < skip_number && value_map_pos[value] == 0)
            value_map_pos[value] = value;
        else if (value_map_pos[value] == 0)
            value_map_pos[value] = (skip_number + cur_pos++);

        positions_data[index] = value_map_pos[value];
    }

    auto unique_values = ColumnUInt64::create(cur_pos);
    auto & unique_values_data = unique_values->getData();

    unique_values_data[0] = values[offset];
    for (size_t i = skip_number; i < value_map_pos.size(); ++i)
        if (auto val = value_map_pos[i])
            unique_values_data[val - skip_number] = static_cast<UInt64>(i - skip_number);

    return {std::move(unique_values), std::move(positions)};
}

template<size_t skip_number>
UniqueValueAndPos collectUniqueValueAndMarkPos(const IColumn & column, size_t offset, size_t limit)
{
    const auto & column_as_uint64 = checkAndGetColumn<ColumnUInt64>(column);
    return collectUniqueValueAndMarkPos<skip_number>(column_as_uint64->getData(), offset, limit);
}

template<size_t skip_number>
ColumnPtr indexAndBuildRelationsPositions(
    const ColumnPtr & relations_, const ColumnPtr & index_, const ColumnArray & origin_positions, size_t offset, size_t limit)
{
    auto res = origin_positions.cloneEmpty();
    const auto & index = checkAndGetColumn<ColumnUInt64>(*index_);

    if (auto * new_relations_positions = typeid_cast<ColumnArray *>(res.get()))
    {
        new_relations_positions->reserve(origin_positions.size());
        new_relations_positions->getData().reserve(origin_positions.getData().size());

        const ColumnUInt64::Container & index_data = index->getData();
        auto callForType = [&new_relations_positions, &index_data, &relations_](auto x)
        {
            using ColumnType = decltype(x);
            if (const auto & relations = checkAndGetColumn<ColumnVector<ColumnType>>(*relations_))
            {
                const typename ColumnVector<ColumnType>::Container & relations_data = relations->getData();
                auto & new_relations_data = typeid_cast<ColumnUInt64 &>(new_relations_positions->getData()).getData();

                new_relations_data.resize(index_data.size());
                for (size_t i = 0; i < index_data.size(); ++i)
                {
                    if (index_data[i] < skip_number)
                        new_relations_data[i] = index_data[i];
                    else
                        new_relations_data[i] = relations_data[index_data[i] - skip_number] + skip_number;
                }
                return true;
            }
            return false;
        };

        if (!callForType(UInt8()) && !callForType(UInt16()) && !callForType(UInt32()) && !callForType(UInt64()))
            throw Exception("LOGICAL ERROR: cannot call for type.", ErrorCodes::LOGICAL_ERROR);

        auto & offsets = new_relations_positions->getOffsets();

        offsets.resize(limit);
        const auto & first_offset = origin_positions.getOffsets()[offset - 1];
        for (size_t i = 0; i < limit; ++i)
            offsets[i] = origin_positions.getOffsets()[offset + i] - first_offset;
    }

    return std::move(res);
}
}


Field ColumnJSONB::operator[](size_t row_num) const
{
    FormatSettings settings{};
    WriteBufferFromOwnString buffer;
    JSONBSerialization::serialize(*this, row_num, JSONBStreamFactory::from<FormatStyle::ESCAPED>(static_cast<WriteBuffer *>(&buffer), settings));
    return Field(buffer.str());
}

void ColumnJSONB::get(size_t rows, Field & res) const
{
    res = operator[](rows);
}

void ColumnJSONB::insertDefault()
{
    /// INSERT Nothing::Nothing to graph and insert empty data ?
    /// Default value is TypeIndex::Nothing
//    for (size_t index = 0; index < mark_columns.size(); ++index)
//        mark_columns[index]->insertDefault();
//
//    for (size_t index = 0; index < data_columns.size(); ++index)
//        data_columns[index]->insertDefault();
}

void ColumnJSONB::insert(const Field & field)
{
    const String & s = DB::get<const String &>(field);
    insertData(s.data(), s.size());
}

void ColumnJSONB::insertData(const char * pos, size_t length)
{
    /// TODO:
    FormatSettings settings{};
    ReadBufferFromMemory buffer(pos, length);
    JSONBSerialization::deserialize(isNullable(), *this, JSONBStreamFactory::from<FormatStyle::ESCAPED>(static_cast<ReadBuffer *>(&buffer), settings));
}

void ColumnJSONB::popBack(size_t n)
{
    std::vector<WrappedPtr> & data_columns = binary_json_data.getAllDataColumns();
    for (size_t index = 0; index < data_columns.size(); ++index)
        data_columns[index]->popBack(n);
}

void ColumnJSONB::insertFrom(const IColumn & src, size_t row_num)
{
    /// TODO: native append insert
    insertRangeFrom(src, row_num, 1);
}

void ColumnJSONB::insertRangeFrom(const IColumn & src, size_t offset, size_t limit)
{
    const auto & source_column = typeid_cast<const ColumnJSONB *>(&src);
    limit = limit && limit < src.size() - offset ? limit : src.size() - offset;
    const auto & new_relations_binary = struct_graph.insertRelationsFrom(
        source_column->getKeysDictionary(), source_column->getRelationsDictionary(),
        *checkAndGetColumn<ColumnArray>(source_column->getRelationsBinary()), offset, limit);

    getRelationsBinary().insertRangeFrom(*new_relations_binary, 0, new_relations_binary->size());
    if (!isMultipleColumn() && !source_column->isMultipleColumn())
        getDataBinary().insertRangeFrom(source_column->getDataBinary(), offset, limit);

    /// TODO:
}

size_t ColumnJSONB::byteSize() const
{
    const std::vector<WrappedPtr> &data_columns = binary_json_data.getAllDataColumns();
    size_t bytes_size = getKeysDictionary().byteSize() + getRelationsDictionary().byteSize();
    for (size_t index = 0; index < data_columns.size(); ++index)
        bytes_size += data_columns[index]->byteSize();
    return bytes_size;
}

size_t ColumnJSONB::allocatedBytes() const
{
    const std::vector<WrappedPtr> &data_columns = binary_json_data.getAllDataColumns();
    size_t allocated_size = getKeysDictionary().byteSize() + getRelationsDictionary().byteSize();
    for (size_t index = 0; index < data_columns.size(); ++index)
        allocated_size += data_columns[index]->allocatedBytes();
    return allocated_size;
}

void ColumnJSONB::updateHashWithValue(size_t /*n*/, SipHash & /*hash*/) const
{
//    for (size_t index = 0; index < mark_columns.size(); ++index)
//        mark_columns[index]->updateHashWithValue(n, hash);
//
//    for (size_t index = 0; index < data_columns.size(); ++index)
//        data_columns[index]->updateHashWithValue(n, hash);
}

ColumnPtr ColumnJSONB::filter(const IColumn::Filter & filter, ssize_t result_size_hint) const
{
    const std::vector<WrappedPtr> & data_columns = binary_json_data.getAllDataColumns();

    Columns binary_data_columns(data_columns.size());
    for (size_t index = 0; index < data_columns.size(); ++index)
        binary_data_columns[index] = data_columns[index]->filter(filter, result_size_hint);

    return ColumnJSONB::create(struct_graph.getKeysAsUniqueColumnPtr(), struct_graph.getRelationsAsUniqueColumnPtr(),
        binary_data_columns, isMultipleColumn(), isNullable());
}

ColumnPtr ColumnJSONB::permute(const IColumn::Permutation & perm, size_t limit) const
{
    const std::vector<WrappedPtr> & data_columns = binary_json_data.getAllDataColumns();

    Columns binary_data_columns(data_columns.size());
    for (size_t index = 0; index < data_columns.size(); ++index)
        binary_data_columns[index] = data_columns[index]->permute(perm, limit);

    return ColumnJSONB::create(struct_graph.getKeysAsUniqueColumnPtr(), struct_graph.getRelationsAsUniqueColumnPtr(),
        binary_data_columns, isMultipleColumn(), isNullable());
}

ColumnPtr ColumnJSONB::index(const IColumn & indexes, size_t limit) const
{
    const std::vector<WrappedPtr> & data_columns = binary_json_data.getAllDataColumns();

    Columns binary_data_columns(data_columns.size());
    for (size_t index = 0; index < data_columns.size(); ++index)
        binary_data_columns[index] = data_columns[index]->index(indexes, limit);

    return ColumnJSONB::create(struct_graph.getKeysAsUniqueColumnPtr(), struct_graph.getRelationsAsUniqueColumnPtr(),
        binary_data_columns, isMultipleColumn(), isNullable());
}

ColumnPtr ColumnJSONB::replicate(const IColumn::Offsets & offsets) const
{
    const std::vector<WrappedPtr> & data_columns = binary_json_data.getAllDataColumns();

    Columns binary_data_columns(data_columns.size());
    for (size_t index = 0; index < data_columns.size(); ++index)
        binary_data_columns[index] = data_columns[index]->replicate(offsets);

    return ColumnJSONB::create(struct_graph.getKeysAsUniqueColumnPtr(), struct_graph.getRelationsAsUniqueColumnPtr(),
        binary_data_columns, isMultipleColumn(), isNullable());
}

std::vector<MutableColumnPtr> ColumnJSONB::scatter(IColumn::ColumnIndex num_columns, const IColumn::Selector & selector) const
{
    const auto & data_columns = binary_json_data.getAllDataColumns();
    std::vector<MutableColumns> scattered_tuple_elements(data_columns.size());

    for (size_t index = 0; index < data_columns.size(); ++index)
        scattered_tuple_elements[index] = data_columns[index]->scatter(num_columns, selector);

    MutableColumns res(num_columns);
    for (size_t scattered_idx = 0; scattered_idx < num_columns; ++scattered_idx)
    {
        Columns new_columns(data_columns.size());
        for (size_t index = 0; index < data_columns.size(); ++index)
            new_columns[index] = std::move(scattered_tuple_elements[index][scattered_idx]);

        MutableColumnPtr key_dictionary = std::move(getKeysDictionary()).mutate();
        MutableColumnPtr relation_dictionary = std::move(getRelationsDictionary()).mutate();
        res[scattered_idx] = ColumnJSONB::create(
            std::move(key_dictionary), std::move(relation_dictionary), new_columns, isMultipleColumn(), isNullable());
    }

    return res;
}

void ColumnJSONB::gather(ColumnGathererStream & /*gatherer_stream*/)
{
    throw Exception("", ErrorCodes::NOT_IMPLEMENTED);
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

StringRef ColumnJSONB::getDataAt(size_t /*n*/) const
{
    /// We don't have a location to store the real string
    throw Exception("Method getDataAt is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

MutableColumnPtr ColumnJSONB::cloneEmpty() const
{
    Columns binary_data_columns(2);
    binary_data_columns[0] = ColumnArray::create(ColumnUInt64::create());
    binary_data_columns[1] = ColumnString::create();
    return ColumnJSONB::create(getKeysDictionaryPtr()->cloneEmpty(), getRelationsDictionaryPtr()->cloneEmpty(),
        std::move(binary_data_columns), false, isNullable(), false);
}

ColumnPtr ColumnJSONB::StructGraph::insertKeysDictionaryFrom(const IColumnUnique & src, const ColumnArray & positions)
{
    constexpr size_t max_mark_size = size_t(JSONBDataMark::End);
    ColumnPtr unique_positions, map_unique_positions;
    const auto & nested_column = positions.getData();
    std::tie(unique_positions, map_unique_positions) = collectUniqueValueAndMarkPos<max_mark_size>(nested_column, 0, nested_column.size());

    const auto & need_insert_keys = src.getNestedColumn()->index(*unique_positions, 0);
    auto new_keys_positions = getKeysAsUniqueColumn().uniqueInsertRangeFrom(*need_insert_keys, 0, need_insert_keys->size());
    return indexAndBuildRelationsPositions<max_mark_size>(std::move(new_keys_positions), map_unique_positions, positions, 0, positions.size());
}

ColumnPtr ColumnJSONB::StructGraph::insertRelationsFrom(
    const IColumnUnique & keys_src, const IColumnUnique & relations_src, const ColumnArray & positions, size_t offset, size_t limit)
{
    ColumnPtr unique_positions, map_unique_positions;
    size_t binary_offset = positions.getOffsets()[offset - 1];  /// flatting
    size_t binary_size = positions.getOffsets()[offset + limit - 1] - binary_offset;    /// flatting
    std::tie(unique_positions, map_unique_positions) = collectUniqueValueAndMarkPos<0>(positions.getData(), binary_offset, binary_size);

    const auto & need_insert_relations = relations_src.getNestedColumn()->index(*unique_positions, 0);
    auto new_relations_data = insertKeysDictionaryFrom(keys_src, *checkAndGetColumn<ColumnArray>(*need_insert_relations));
    auto new_relations_positions = getRelationsAsUniqueColumn().uniqueInsertRangeFrom(*new_relations_data, 0, new_relations_data->size());
    return indexAndBuildRelationsPositions<0>(std::move(new_relations_positions), map_unique_positions, positions, offset, limit);
}

IColumn & ColumnJSONB::BinaryJSONData::getBinaryColumn()
{
    if (multiple_columns || data_columns.size() != 2)
        throw Exception("JSONB column status error, the multiple_columns must be false and data_column size must be 2.", ErrorCodes::ILLEGAL_COLUMN);
    return *data_columns[1];
}

const IColumn & ColumnJSONB::BinaryJSONData::getBinaryColumn() const
{
    if (multiple_columns || data_columns.size() != 2)
        throw Exception("JSONB column status error, the multiple_columns must be false and data_column size must be 2.", ErrorCodes::ILLEGAL_COLUMN);
    return *data_columns[1];
}

ColumnPtr & ColumnJSONB::BinaryJSONData::getBinaryColumnPtr()
{
    if (multiple_columns || data_columns.size() != 2)
        throw Exception("JSONB column status error, the multiple_columns must be false and data_column size must be 2.", ErrorCodes::ILLEGAL_COLUMN);
    return data_columns[1];
}

const ColumnPtr & ColumnJSONB::BinaryJSONData::getBinaryColumnPtr() const
{
    if (multiple_columns || data_columns.size() != 2)
        throw Exception("JSONB column status error, the multiple_columns must be false and data_column size must be 2.", ErrorCodes::ILLEGAL_COLUMN);
    return data_columns[1];
}

ColumnJSONB::Ptr ColumnJSONB::convertToMultipleIfNeed(size_t offset, size_t limit) const
{
    Columns binary_data_columns(2);
    binary_data_columns[0] = ColumnArray::create(ColumnUInt64::create());
    binary_data_columns[1] = ColumnString::create();
    MutablePtr empty_column = ColumnJSONB::create(
        getKeysDictionaryPtr()->cloneEmpty(), getRelationsDictionaryPtr()->cloneEmpty(),
        std::move(binary_data_columns), false, isNullable(), false);

    empty_column->insertRangeFrom(*this, offset, limit);
    return empty_column;
}

ColumnJSONB::BinaryJSONData::BinaryJSONData(
    MutableColumns && data_columns_, bool multiple_columns_, bool is_nullable_, bool is_low_cardinality_)
    : is_nullable(is_nullable_), is_low_cardinality(is_low_cardinality_), multiple_columns(multiple_columns_)
{
    data_columns.reserve(data_columns_.size());
    for (auto & data_column : data_columns_)
    {
        if (isColumnConst(*data_column))
            throw Exception{"ColumnJSONB cannot have ColumnConst as its element", ErrorCodes::ILLEGAL_COLUMN};

        data_columns.push_back(std::move(data_column));
    }
}

ColumnJSONB::ColumnJSONB(
    MutableColumnPtr && keys_dictionary_, MutableColumnPtr && relations_dictionary_, MutableColumns && data_columns_,
    bool multiple_columns, bool is_nullable_, bool is_lowCardinality_)
    : struct_graph(std::move(keys_dictionary_), std::move(relations_dictionary_)),
      binary_json_data(std::move(data_columns_), multiple_columns, is_nullable_, is_lowCardinality_)
{
}

ColumnJSONB::MutablePtr ColumnJSONB::create(
    const ColumnPtr & keys_dictionary_, const ColumnPtr & relations_dictionary_, const Columns & data_columns,
    bool multiple_columns, bool is_nullable_, bool is_lowCardinality_)
{
    MutableColumns mutable_data_columns(data_columns.size());
    for (size_t index = 0; index < data_columns.size(); ++index)
        mutable_data_columns[index] = data_columns[index]->assumeMutable();

    return Base::create(keys_dictionary_->assumeMutable(), relations_dictionary_->assumeMutable(),
        std::move(mutable_data_columns), multiple_columns, is_nullable_, is_lowCardinality_);
}

}
