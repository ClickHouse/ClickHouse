#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnIndex.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Common/VectorWithMemoryTracking.h>

#include <vector>

namespace DB
{

namespace BucketedMapLowCardinality
{

inline bool collectDataFromBuckets(
    const VectorWithMemoryTracking<ColumnPtr> & data_buckets,
    const std::vector<const ColumnArray::Offsets *> & offsets_buckets,
    size_t num_rows,
    IColumn & data,
    ColumnArray::Offsets * output_offsets = nullptr)
{
    auto * low_cardinality_data = typeid_cast<ColumnLowCardinality *>(&data);
    if (!low_cardinality_data)
        return false;

    VectorWithMemoryTracking<ColumnPtr> dictionary_columns;
    std::vector<ColumnPtr> bucket_index_columns;
    dictionary_columns.reserve(data_buckets.size());
    bucket_index_columns.reserve(data_buckets.size());

    /// Convert each bucket to a compact dictionary + indexes pair.
    size_t total_indexes = 0;
    for (const auto & data_bucket : data_buckets)
    {
        const auto * low_cardinality_bucket = typeid_cast<const ColumnLowCardinality *>(data_bucket.get());
        if (!low_cardinality_bucket)
            return false;

        auto dictionary_encoded = low_cardinality_bucket->getMinimalDictionaryEncodedColumn(0, low_cardinality_bucket->size());
        total_indexes += dictionary_encoded.indexes->size();
        dictionary_columns.push_back(std::move(dictionary_encoded.dictionary));
        bucket_index_columns.push_back(std::move(dictionary_encoded.indexes));
    }

    /// Concatenate bucket dictionaries into one temporary dictionary.
    auto dictionary = low_cardinality_data->getDictionary().getNestedColumn()->cloneEmpty();
    dictionary->prepareForSquashing(dictionary_columns, 1);

    std::vector<size_t> dictionary_offsets;
    dictionary_offsets.reserve(dictionary_columns.size());
    for (const auto & dictionary_column : dictionary_columns)
    {
        dictionary_offsets.push_back(dictionary->size());
        dictionary->insertRangeFrom(*dictionary_column, 0, dictionary_column->size());
    }

    /// Rebuild the row/bucket order as indexes into the temporary dictionary.
    /// If `output_offsets` is provided, append output offsets in the same pass.
    ColumnIndex indexes;
    indexes.reserve(total_indexes);

    size_t current_offset = output_offsets && !output_offsets->empty() ? output_offsets->back() : 0;
    for (size_t row = 0; row != num_rows; ++row)
    {
        for (size_t bucket = 0; bucket != data_buckets.size(); ++bucket)
        {
            const auto & offsets = *offsets_buckets[bucket];
            size_t offset_start = offsets[ssize_t(row) - 1];
            size_t offset_end = offsets[row];
            size_t length = offset_end - offset_start;
            const auto & bucket_indexes = *bucket_index_columns[bucket];
            size_t dictionary_offset = dictionary_offsets[bucket];

            if (length)
            {
                size_t bucket_dictionary_size = dictionary_columns[bucket]->size();
                indexes.insertIndexesRangeWithShift(
                    bucket_indexes, offset_start, length, dictionary_offset, dictionary_offset + bucket_dictionary_size - 1);
            }

            if (output_offsets)
                current_offset += length;
        }

        if (output_offsets)
            output_offsets->push_back(current_offset);
    }

    /// Merge the temporary dictionary-encoded stream into the output LC dictionary.
    low_cardinality_data->insertRangeFromDictionaryEncodedColumn(*dictionary, *indexes.getIndexes());
    return true;
}

}

}
