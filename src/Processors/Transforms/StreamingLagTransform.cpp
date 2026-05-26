#include <Processors/Transforms/StreamingLagTransform.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <Common/SipHash.h>
#include <DataTypes/IDataType.h>

namespace DB
{

StreamingLagTransform::StreamingLagTransform(
    const SharedHeader & input_header_,
    SharedHeader output_header_,
    SortDescription prefix_desc,
    std::vector<std::string> suffix_partition_col_names_,
    std::vector<std::string> value_col_names_,
    std::vector<std::string> output_col_names_,
    std::vector<std::optional<Field>> default_values)
    : ISimpleTransform(input_header_, output_header_, false)
    , prefix_description_(std::move(prefix_desc))
    , default_values_(std::move(default_values))
{
    const Block & in_header = *input_header_;
    const Block & out_header = *output_header_;

    for (const auto & desc : prefix_description_)
        prefix_col_indices_.push_back(in_header.getPositionByName(desc.column_name));

    for (const auto & name : suffix_partition_col_names_)
        suffix_col_indices_.push_back(in_header.getPositionByName(name));

    for (const auto & name : value_col_names_)
        value_col_indices_.push_back(in_header.getPositionByName(name));

    for (const auto & name : output_col_names_)
    {
        const auto & col_with_type = out_header.getByName(name);
        result_types_.push_back(col_with_type.type);
        result_names_.push_back(col_with_type.name);
    }
}

std::optional<StreamingLagTransform::MapColView>
StreamingLagTransform::tryGetMapView(const Block & block, size_t col_idx)
{
    const IColumn & raw_col = *block.getByPosition(col_idx).column;
    const auto * col_array = typeid_cast<const ColumnArray *>(&raw_col);
    if (!col_array)
        return std::nullopt;

    const auto * col_tuple = typeid_cast<const ColumnTuple *>(&col_array->getData());
    if (!col_tuple || col_tuple->tupleSize() != 2)
        return std::nullopt;

    const auto * col_keys_lc = typeid_cast<const ColumnLowCardinality *>(&col_tuple->getColumn(0));
    if (!col_keys_lc)
        return std::nullopt;

    const auto * col_keys_dict = typeid_cast<const ColumnString *>(col_keys_lc->getDictionary().getNestedColumn().get());
    if (!col_keys_dict)
        return std::nullopt;

    const auto * col_vals = typeid_cast<const ColumnString *>(&col_tuple->getColumn(1));
    if (!col_vals)
        return std::nullopt;

    MapColView view;
    view.array_offsets = &col_array->getOffsets();
    view.key_chars = &col_keys_dict->getChars();
    view.key_str_offsets = &col_keys_dict->getOffsets();
    view.val_chars = &col_vals->getChars();
    view.val_str_offsets = &col_vals->getOffsets();

    const IColumn & idx_col = col_keys_lc->getIndexes();
    if (const auto * u8 = typeid_cast<const ColumnUInt8 *>(&idx_col))
        view.idx_u8 = u8->getData().data();
    else if (const auto * u16 = typeid_cast<const ColumnUInt16 *>(&idx_col))
        view.idx_u16 = u16->getData().data();
    else if (const auto * u32 = typeid_cast<const ColumnUInt32 *>(&idx_col))
        view.idx_u32 = u32->getData().data();
    else
        return std::nullopt;

    return view;
}

void StreamingLagTransform::hashMapRow(const MapColView & view, size_t row, SipHash & hash)
{
    const size_t start = row > 0 ? (*view.array_offsets)[row - 1] : 0;
    const size_t end = (*view.array_offsets)[row];

    for (size_t i = start; i < end; ++i)
    {
        size_t key_idx;
        if (view.idx_u8)
            key_idx = view.idx_u8[i];
        else if (view.idx_u16)
            key_idx = view.idx_u16[i];
        else
            key_idx = view.idx_u32[i];

        const size_t key_start = key_idx > 0 ? (*view.key_str_offsets)[key_idx - 1] : 0;
        const size_t key_end = (*view.key_str_offsets)[key_idx];
        hash.update(reinterpret_cast<const char *>(view.key_chars->data() + key_start), key_end - key_start);

        const size_t val_start = i > 0 ? (*view.val_str_offsets)[i - 1] : 0;
        const size_t val_end = (*view.val_str_offsets)[i];
        hash.update(reinterpret_cast<const char *>(view.val_chars->data() + val_start), val_end - val_start);
    }
}

void StreamingLagTransform::transform(Chunk & chunk)
{
    const size_t num_rows = chunk.getNumRows();
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    const size_t num_funcs = value_col_indices_.size();

    // Pre-build Map column views for all suffix partition columns once per chunk.
    std::vector<std::optional<MapColView>> suffix_views;
    suffix_views.reserve(suffix_col_indices_.size());
    for (size_t idx : suffix_col_indices_)
        suffix_views.push_back(tryGetMapView(block, idx));

    std::vector<MutableColumnPtr> result_cols;
    result_cols.reserve(num_funcs);
    for (size_t fi = 0; fi < num_funcs; ++fi)
    {
        result_cols.push_back(result_types_[fi]->createColumn());
        result_cols.back()->reserve(num_rows);
    }

    for (size_t row = 0; row < num_rows; ++row)
    {
        SipHash prefix_hasher;
        for (size_t idx : prefix_col_indices_)
            block.getByPosition(idx).column->updateHashWithValue(row, prefix_hasher);
        const UInt128 prefix_hash = prefix_hasher.get128();

        if (first_row_ || prefix_hash != current_prefix_hash_)
        {
            state_map_.clear();
            current_prefix_hash_ = prefix_hash;
            first_row_ = false;
        }

        SipHash suffix_hasher;
        for (size_t si = 0; si < suffix_col_indices_.size(); ++si)
        {
            if (suffix_views[si])
                hashMapRow(*suffix_views[si], row, suffix_hasher);
            else
                block.getByPosition(suffix_col_indices_[si]).column->updateHashWithValue(row, suffix_hasher);
        }
        const UInt128 suffix_key = suffix_hasher.get128();

        auto it = state_map_.find(suffix_key);

        for (size_t fi = 0; fi < num_funcs; ++fi)
        {
            if (it != state_map_.end())
                result_cols[fi]->insert(it->second[fi]);
            else if (default_values_[fi])
                result_cols[fi]->insert(*default_values_[fi]);
            else
                result_cols[fi]->insertDefault();
        }

        if (it == state_map_.end())
        {
            auto [new_it, inserted] = state_map_.emplace(suffix_key, std::vector<Field>(num_funcs));
            (void)inserted;
            it = new_it;
        }
        for (size_t fi = 0; fi < num_funcs; ++fi)
            block.getByPosition(value_col_indices_[fi]).column->get(row, it->second[fi]);
    }

    for (size_t fi = 0; fi < num_funcs; ++fi)
        block.insert(ColumnWithTypeAndName{std::move(result_cols[fi]), result_types_[fi], result_names_[fi]});

    chunk.setColumns(block.getColumns(), num_rows);
}

}
