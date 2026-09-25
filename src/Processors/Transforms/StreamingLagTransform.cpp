#include <Processors/Transforms/StreamingLagTransform.h>

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


void StreamingLagTransform::transform(Chunk & chunk)
{
    const size_t num_rows = chunk.getNumRows();
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    const size_t num_funcs = value_col_indices_.size();

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
        for (size_t idx : suffix_col_indices_)
            block.getByPosition(idx).column->updateHashWithValue(row, suffix_hasher);
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
