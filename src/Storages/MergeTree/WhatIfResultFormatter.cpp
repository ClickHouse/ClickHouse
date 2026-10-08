#include <Storages/MergeTree/WhatIfIndexEstimator.h>

#include <IO/WriteHelpers.h>
#include <Common/formatReadable.h>

#include <fmt/format.h>

namespace DB
{

void WhatIfIndexEstimator::Result::format(WriteBuffer & out) const
{
    writeCString("Baseline (after PK + partition + existing indexes):\n", out);
    writeString(fmt::format("  table:       {}.{}\n", database, table), out);
    writeString(fmt::format("  parts:       {}\n", baseline_parts), out);
    writeString(fmt::format("  marks:       {}\n", baseline_marks), out);
    if (baseline_est_bytes > 0)
        writeString(fmt::format("  est_bytes:   {}\n", ReadableSize(baseline_est_bytes)), out);
    writeCString("\n", out);

    for (const auto & idx : index_results)
    {
        if (!idx.index_type.empty())
            writeString(fmt::format("With {} ({}, hypothetical):\n", idx.index_name, idx.index_type), out);
        else
            writeString(fmt::format("{}:\n", idx.index_name), out);

        if (idx.status == IndexResult::NotApplicable)
        {
            writeCString("  status:       not_applicable\n", out);
            writeString(fmt::format("  reason:       {}\n", idx.not_applicable_reason), out);
            writeCString("\n", out);
            continue;
        }

        writeCString("  status:       applicable\n", out);
        writeString(fmt::format("  marks:        {}\n", idx.estimated_marks), out);

        if (baseline_marks > 0 && baseline_est_bytes > 0)
        {
            UInt64 hypo_bytes = static_cast<UInt64>(
                static_cast<double>(baseline_est_bytes) * static_cast<double>(idx.estimated_marks) / static_cast<double>(baseline_marks));
            writeString(fmt::format("  est_bytes:    {}\n", ReadableSize(hypo_bytes)), out);
        }

        writeString(fmt::format("  skip_ratio:   {:.1f}%\n", idx.skip_ratio * 100.0), out);
        writeCString("\n", out);

        writeCString("Estimation:\n", out);
        writeString(fmt::format("  source:           {}\n", idx.estimate_source), out);

        String empirical_status_str;
        switch (idx.empirical_status)
        {
            case IndexResult::Ok: empirical_status_str = "ok"; break;
            case IndexResult::Unsupported: empirical_status_str = "unsupported"; break;
            case IndexResult::Disabled: empirical_status_str = "disabled"; break;
        }
        writeString(fmt::format("  empirical_status: {}\n", empirical_status_str), out);

        if (idx.empirical_status == IndexResult::Ok)
        {
            writeString(fmt::format("  sampled_parts:    {} / {}\n", idx.sampled_parts, idx.total_parts), out);
            writeString(fmt::format("  sampled_marks:    {} / {}\n", idx.sampled_marks, idx.total_marks), out);
            writeString(fmt::format("  elapsed_us:       {}\n", idx.elapsed_us), out);
        }
        writeCString("\n", out);
    }
}

}
