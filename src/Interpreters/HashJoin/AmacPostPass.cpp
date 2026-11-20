#include <Interpreters/HashJoin/JoinRightColumns.h>

namespace DB::JoinProbe
{
namespace
{

/// Each difference between consecutive offsets is the number of output rows for one left input.
template <bool groups, bool outer, bool keep_unmatched = false>
size_t finishAmacMatches(JoinRightColumnsData & data)
{
    auto & added = data.added_columns;
    auto & output = added.lazy_output;
    auto & refs = output.row_refs;
    const size_t rows = data.batch_rows;
    added.offsets_to_replicate.resize(rows);
    if constexpr (groups)
    {
        if (added.need_filter)
            added.filter.resize(rows);
    }
    else
        output.probe_output = ProbeOutput::FilteredOffsets;

    size_t kept = 0;
    size_t total = 0;
    size_t defaults = 0;
    for (size_t i = 0; i < rows; ++i)
    {
        const MatchWord word = refs[i];
        if constexpr (groups)
        {
            if (added.need_filter)
                added.filter[i] = word != 0;
            const size_t copies = word ? refWordRows(word) : outer;
            total += copies;
            if constexpr (outer)
                defaults += !word;
        }
        else
            total += word != 0;
        added.offsets_to_replicate[i] = total;

        if constexpr (!outer)
        {
            if (word)
            {
                if (added.record_row_refs)
                {
                    if constexpr (keep_unmatched)
                        refs[kept] = 0;
                    else if (kept != i)
                        refs[kept] = word;
                }
                ++kept;
            }
        }
    }
    if (added.record_row_refs)
    {
        if constexpr (!outer)
            refs.resize_assume_reserved(kept);
        output.row_count = total;
        output.hash_table_matches = keep_unmatched ? 0 : total - defaults;
#ifndef NDEBUG
        for (MatchWord word : refs)
            if (word)
                added.checkColumnsOfRef(word);
#endif
    }
    else
        refs.clear();
    output.output_by_row_list = groups;
    return rows;
}

}

/// Produce the same filter, left-row indexes and right refs as the in-order inner join.
size_t compactOrderedAmacRows(JoinRightColumnsData & data)
{
    auto & added = data.added_columns;
    auto & output = added.lazy_output;
    auto & refs = output.row_refs;
    const size_t rows = data.batch_rows;
    chassert(added.need_filter);
    added.filter.resize(rows);
    added.matched_rows.resize(rows);

    size_t kept = 0;
    const bool record_refs = added.record_row_refs;
    for (size_t i = 0; i < rows; ++i)
    {
        const MatchWord word = refs[i];
        const bool hit = word != 0;
        added.filter[i] = hit;
        added.matched_rows[kept] = i;
        if (record_refs)
            refs[kept] = word;
        kept += hit;
    }

    added.matched_rows.resize_assume_reserved(kept);
    refs.resize_assume_reserved(record_refs ? kept : 0);
    if (record_refs)
    {
        output.row_count = kept;
        output.hash_table_matches = kept;
#ifndef NDEBUG
        for (MatchWord word : refs)
            added.checkColumnsOfRef(word);
#endif
    }
    return rows;
}

AddMatchesFn selectAmacPostPass(bool groups, bool add_missing, bool keeps_unmatched)
{
    if (groups)
        return add_missing ? &finishAmacMatches<true, true> : &finishAmacMatches<true, false>;
    return keeps_unmatched ? &finishAmacMatches<false, false, true> : &finishAmacMatches<false, false>;
}
}
