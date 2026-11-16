#pragma once

#include <Processors/Formats/Impl/Puffin/PuffinBlobBuilder.h>

namespace DB
{

PuffinBlobBuilderPtr createPuffinThetaSketchBuilder(const Block & header, const FormatSettings & format_settings);

}
