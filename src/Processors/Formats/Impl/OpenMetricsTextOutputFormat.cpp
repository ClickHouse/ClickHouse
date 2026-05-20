#include <Processors/Formats/Impl/OpenMetricsTextOutputFormat.h>

#include <Formats/FormatFactory.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace
{
constexpr auto FORMAT_NAME = "OpenMetrics";
}

OpenMetricsTextOutputFormat::OpenMetricsTextOutputFormat(
    WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_)
    : PrometheusTextOutputFormat(out_, std::move(header_), format_settings_)
{
    bindUnitColumnIfPresent(getPort(PortKind::Main).getHeader());
}

void OpenMetricsTextOutputFormat::writeAdditionalFamilyMetadata()
{
    if (current_metric.unit.empty())
        return;

    writeCString("# UNIT ", out);
    writeString(current_metric.name, out);
    writeChar(' ', out);
    writeString(current_metric.unit, out);
    writeChar('\n', out);
}

void OpenMetricsTextOutputFormat::finalizeImpl()
{
    PrometheusTextOutputFormat::finalizeImpl();
    writeCString("# EOF\n", out);
}

void registerOutputFormatOpenMetrics(FormatFactory & factory)
{
    factory.registerOutputFormat(
        FORMAT_NAME,
        [](WriteBuffer & buf, const Block & sample, const FormatSettings & settings, FormatFilterInfoPtr /*format_filter_info*/)
        { return std::make_shared<OpenMetricsTextOutputFormat>(buf, std::make_shared<const Block>(sample), settings); });

    factory.setContentType(FORMAT_NAME, "application/openmetrics-text; version=1.0.0; charset=utf-8");
}

}
