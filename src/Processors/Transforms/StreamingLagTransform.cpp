#include <Processors/Transforms/StreamingLagTransform.h>

#include <bit>
#include <cstring>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <Common/SipHash.h>
#include <DataTypes/IDataType.h>

namespace DB
{

/// Hash a single column value using compareAt-compatible semantics.
/// For Float32/Float64, canonicalizes NaN payloads and signed zero so that
/// logically equal values (as defined by compareAt) always produce the same hash.
static void hashColumnValueCanonical(const IColumn & col, size_t row, SipHash & hash)
{
    if (const auto * f64 = typeid_cast<const ColumnVector<Float64> *>(&col))
    {
        Float64 v = f64->getData()[row];
        UInt64 bits = std::bit_cast<UInt64>(v);
        if ((bits & 0x7FF0000000000000ULL) == 0x7FF0000000000000ULL && (bits & 0x000FFFFFFFFFFFFFULL) != 0)
            bits = 0x7FF8000000000000ULL; /// canonical quiet NaN
        else if (bits == 0x8000000000000000ULL)
            bits = 0x0ULL;               /// -0.0 → +0.0
        hash.update(bits);
    }
    else if (const auto * f32 = typeid_cast<const ColumnVector<Float32> *>(&col))
    {
        Float32 v = f32->getData()[row];
        UInt32 bits = std::bit_cast<UInt32>(v);
        if ((bits & 0x7F800000UL) == 0x7F800000UL && (bits & 0x007FFFFFUL) != 0)
            bits = 0x7FC00000UL;         /// canonical quiet NaN
        else if (bits == 0x80000000UL)
            bits = 0x0UL;               /// -0.0 → +0.0
        hash.update(bits);
    }
    else if (const auto * nullable = typeid_cast<const ColumnNullable *>(&col))
    {
        const UInt8 is_null = nullable->getNullMapData()[row];
        hash.update(is_null);
        if (!is_null)
            hashColumnValueCanonical(nullable->getNestedColumn(), row, hash);
    }
    else if (const auto * lc = typeid_cast<const ColumnLowCardinality *>(&col))
    {
        hashColumnValueCanonical(*lc->getDictionary().getNestedColumn(),
                                 lc->getIndexes().getUInt(row), hash);
    }
    else
    {
        col.updateHashWithValue(row, hash);
    }
}

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
            hashColumnValueCanonical(*block.getByPosition(idx).column, row, prefix_hasher);
        const UInt128 prefix_hash = prefix_hasher.get128();

        if (first_row_ || prefix_hash != current_prefix_hash_)
        {
            state_map_.clear();
            current_prefix_hash_ = prefix_hash;
            first_row_ = false;
        }

        SipHash suffix_hasher;
        for (size_t idx : suffix_col_indices_)
            hashColumnValueCanonical(*block.getByPosition(idx).column, row, suffix_hasher);
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
