#include <Processors/QueryPlan/Profiling/Render/JsonPlanRenderer.h>

#include <Common/JSONBuilder.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteBuffer.h>
#include <Processors/QueryPlan/QueryPlanToJSON.h>

namespace DB
{

void JsonPlanRenderer::render(const CapturedPlan & captured, WriteBuffer & out)
{
    FormatSettings format_settings;
    /// The consumer of this column is a program, and a 64-bit integer quoted as a string is one
    /// more thing for it to undo before it can aggregate.
    format_settings.json.quote_64bit_integers = false;

    JSONBuilder::FormatSettings json_format_settings{.settings = format_settings};
    JSONBuilder::FormatContext format_context{.out = out};
    capturedPlanToJSON(captured)->format(json_format_settings, format_context);
}

}
