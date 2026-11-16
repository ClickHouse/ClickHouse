#include <Processors/Formats/Impl/Puffin/PuffinBlobBuilder.h>
#include <Processors/Formats/Impl/Puffin/PuffinCommon.h>
#include <Processors/Formats/Impl/Puffin/PuffinDeletionVector.h>
#include <Processors/Formats/Impl/Puffin/PuffinThetaSketch.h>

#include <Common/Exception.h>
#include <Formats/FormatSettings.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

PuffinBlobBuilderPtr createPuffinBlobBuilder(const Block & header, const FormatSettings & format_settings)
{
    const String & blob_type = format_settings.puffin.output_blob_type;

    if (blob_type == PUFFIN_DELETION_VECTOR_BLOB_TYPE)
        return createPuffinDeletionVectorBuilder(header);

    if (blob_type == PUFFIN_DATASKETCHES_THETA_BLOB_TYPE)
        return createPuffinThetaSketchBuilder(header, format_settings);

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Unknown Puffin blob type '{}' in setting `output_format_puffin_blob_type`, supported types: '{}', '{}'",
        blob_type,
        PUFFIN_DELETION_VECTOR_BLOB_TYPE,
        PUFFIN_DATASKETCHES_THETA_BLOB_TYPE);
}

}
