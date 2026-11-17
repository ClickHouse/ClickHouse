#include <Processors/Formats/Impl/Puffin/PuffinBlobBuilder.h>
#include <Processors/Formats/Impl/Puffin/PuffinCommon.h>
#include <Processors/Formats/Impl/Puffin/PuffinDeletionVector.h>
#include <Processors/Formats/Impl/Puffin/PuffinThetaSketch.h>

#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadHelpers.h>
#include <Common/Exception.h>
#include <Common/UnorderedSetWithMemoryTracking.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

std::vector<Int32> parseFieldIds(const String & setting, const Block & header)
{
    if (setting.empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Setting `output_format_puffin_field_ids` must be set to the Iceberg field ids of the columns to write Theta sketches");

    std::vector<Int32> field_ids;
    std::vector<String> parts;
    boost::split(parts, setting, boost::is_any_of(","));
    UnorderedSetWithMemoryTracking<Int32> seen;
    for (auto & part : parts)
    {
        boost::trim(part);
        Int32 field_id = 0;
        if (!tryParse(field_id, part) || field_id <= 0)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Setting `output_format_puffin_field_ids` must be a comma-separated list of positive integers, got '{}'",
                setting);
        if (!seen.insert(field_id).second)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Setting `output_format_puffin_field_ids` contains field id {} twice", field_id);
        field_ids.push_back(field_id);
    }

    if (field_ids.size() != header.columns())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Setting `output_format_puffin_field_ids` has {} field ids, but {} columns are written: {}",
            field_ids.size(),
            header.columns(),
            header.dumpNames());
    return field_ids;
}

PuffinThetaSketchBlobOptions getThetaSketchBlobOptions(const Block & header, const FormatSettings & format_settings)
{
    PuffinThetaSketchBlobOptions options{
        .field_ids = parseFieldIds(format_settings.puffin.output_field_ids, header),
        .snapshot_id = format_settings.puffin.output_snapshot_id,
        .sequence_number = format_settings.puffin.output_sequence_number,
    };
    if (options.snapshot_id == -1)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Setting `output_format_puffin_snapshot_id` must be set to the snapshot the Theta sketches are computed from");
    if (options.sequence_number == -1)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Setting `output_format_puffin_sequence_number` must be set to the sequence number of the snapshot the Theta sketches are computed from");
    return options;
}

}

PuffinBlobBuilderPtr createPuffinBlobBuilder(const Block & header, const FormatSettings & format_settings)
{
    const String & blob_type = format_settings.puffin.output_blob_type;

    if (blob_type == PUFFIN_DELETION_VECTOR_BLOB_TYPE)
        return createPuffinDeletionVectorBuilder(header);

    if (blob_type == PUFFIN_DATASKETCHES_THETA_BLOB_TYPE)
        return createPuffinThetaSketchBuilder(header, getThetaSketchBlobOptions(header, format_settings));

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Unknown Puffin blob type '{}' in setting `output_format_puffin_blob_type`, supported types: '{}', '{}'",
        blob_type,
        PUFFIN_DELETION_VECTOR_BLOB_TYPE,
        PUFFIN_DATASKETCHES_THETA_BLOB_TYPE);
}

}
