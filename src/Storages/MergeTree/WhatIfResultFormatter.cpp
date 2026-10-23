#include <Storages/MergeTree/WhatIfResult.h>

#include <Core/Block.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteHelpers.h>
#include <Common/formatReadable.h>

#include <fmt/format.h>

namespace DB
{

namespace
{

const char * sourceName(WhatIfCandidateResult::EstimateSource source)
{
    switch (source)
    {
        case WhatIfCandidateResult::Empirical: return "empirical";
        case WhatIfCandidateResult::Statistical: return "statistical";
        case WhatIfCandidateResult::ApplicabilityOnly: return "applicability_only";
    }
}

}

void WhatIfResult::format(WriteBuffer & out) const
{
    writeCString("Baseline (after PK + partition + existing indexes):\n", out);
    writeString(fmt::format("  table:       {}.{}\n", database, table), out);
    writeString(fmt::format("  parts:       {}\n", baseline_parts), out);
    writeString(fmt::format("  marks:       {}\n", baseline_marks), out);
    writeString(fmt::format("  rows:        {}\n", baseline_rows), out);
    if (baseline_est_bytes > 0)
        writeString(fmt::format("  est_bytes:   {}\n", ReadableSize(baseline_est_bytes)), out);
    writeCString("\n", out);

    for (const auto & idx : candidates)
    {
        if (!idx.type.empty())
            writeString(fmt::format("With {} ({}, hypothetical):\n", idx.name, idx.type), out);
        else
            writeString(fmt::format("{}:\n", idx.name), out);

        if (idx.status == WhatIfCandidateResult::NotApplicable)
        {
            writeCString("  status:       not_applicable\n", out);
            writeString(fmt::format("  reason:       {}\n", idx.not_applicable_reason), out);
            writeCString("\n", out);
            continue;
        }

        writeCString("  status:       applicable\n", out);
        if (idx.estimated_marks)
            writeString(fmt::format("  marks:        {}\n", *idx.estimated_marks), out);
        if (idx.estimated_rows)
            writeString(fmt::format("  rows:         {}\n", *idx.estimated_rows), out);
        /// the layout a projection part would get is not recorded in a part, so the estimate can only span them
        if (idx.estimated_marks_high > idx.estimated_marks_low)
            writeString(
                fmt::format("  marks_span:   {} to {}\n", idx.estimated_marks_low, idx.estimated_marks_high), out);

        /// only for indexes
        if (idx.kind == WhatIfCandidateResult::Index && idx.estimated_marks && baseline_marks > 0 && baseline_est_bytes > 0)
        {
            UInt64 hypo_bytes = static_cast<UInt64>(
                static_cast<double>(baseline_est_bytes) * static_cast<double>(*idx.estimated_marks) / static_cast<double>(baseline_marks));
            writeString(fmt::format("  est_bytes:    {}\n", ReadableSize(hypo_bytes)), out);
        }

        /// a projection can read more than the base table, and a ratio reads better than a negative skip
        if (idx.kind == WhatIfCandidateResult::Projection)
        {
            if (idx.estimated_marks && baseline_marks > 0)
            {
                const double read_ratio = static_cast<double>(*idx.estimated_marks) / static_cast<double>(baseline_marks);
                writeString(fmt::format("  read_ratio:   {:.2f}x\n", read_ratio), out);
            }
        }
        else if (idx.estimated_marks)
            writeString(fmt::format("  skip_ratio:   {:.1f}%\n", idx.skip_ratio * 100.0), out);

        if (!idx.verdict.empty())
            writeString(fmt::format("  verdict:      {}\n", idx.verdict), out);
        if (!idx.verdict_reason.empty())
            writeString(fmt::format("  reason:       {}\n", idx.verdict_reason), out);
        writeCString("\n", out);

        writeCString("Estimation:\n", out);

        writeString(fmt::format("  source:           {}\n", sourceName(idx.estimate_source)), out);

        String empirical_status_str;
        switch (idx.empirical_status)
        {
            case WhatIfCandidateResult::Ok: empirical_status_str = "ok"; break;
            case WhatIfCandidateResult::Unsupported: empirical_status_str = "unsupported"; break;
            case WhatIfCandidateResult::Disabled: empirical_status_str = "disabled"; break;
        }
        writeString(fmt::format("  empirical_status: {}\n", empirical_status_str), out);

        if (idx.empirical_status == WhatIfCandidateResult::Unsupported)
        {
            String reason = idx.empirical_unsupported_reason;
            if (reason.empty())
                reason = "The empirical estimate could not be produced for this candidate";
            writeString(fmt::format("  empirical_reason: {}\n", reason), out);
        }

        if (idx.empirical_status == WhatIfCandidateResult::Ok)
        {
            writeString(fmt::format("  sampled_parts:    {} / {}\n", idx.sampled_parts, idx.total_parts), out);
            writeString(fmt::format("  sampled_marks:    {} / {}\n", idx.sampled_marks, idx.total_marks), out);
            writeString(fmt::format("  elapsed_us:       {}\n", idx.elapsed_us), out);
        }
        writeCString("\n", out);
    }
}

Block WhatIfResult::tableHeader()
{
    const auto string = std::make_shared<DataTypeString>();
    const auto count = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>());
    return Block{
        {string->createColumn(), string, "name"},
        {string->createColumn(), string, "kind"},
        {string->createColumn(), string, "status"},
        {count->createColumn(), count, "marks"},
        {count->createColumn(), count, "marks_low"},
        {count->createColumn(), count, "marks_high"},
        {count->createColumn(), count, "rows"},
        {string->createColumn(), string, "verdict"},
        {string->createColumn(), string, "source"},
        {string->createColumn(), string, "reason"},
    };
}

void WhatIfResult::fillTable(MutableColumns & columns) const
{
    for (const auto & candidate : candidates)
    {
        const bool applicable = candidate.status == WhatIfCandidateResult::Applicable;
        const auto & marks = candidate.estimated_marks;
        const bool has_span = marks && candidate.estimated_marks_high > candidate.estimated_marks_low;
        auto count = [](std::optional<UInt64> value) { return value ? Field(*value) : Field(); };

        columns[0]->insert(candidate.name);
        columns[1]->insert(candidate.kind == WhatIfCandidateResult::Index ? "index" : "projection");
        columns[2]->insert(applicable ? "applicable" : "not_applicable");
        columns[3]->insert(count(marks));
        columns[4]->insert(has_span ? Field(candidate.estimated_marks_low) : count(marks));
        columns[5]->insert(has_span ? Field(candidate.estimated_marks_high) : count(marks));
        columns[6]->insert(count(candidate.estimated_rows));
        columns[7]->insert(candidate.verdict);
        columns[8]->insert(applicable ? sourceName(candidate.estimate_source) : "");
        columns[9]->insert(applicable ? candidate.verdict_reason : candidate.not_applicable_reason);
    }
}

}
