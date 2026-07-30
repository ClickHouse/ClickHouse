#include <Core/Streaming/Settings.h>

#include <Parsers/IAST.h>

namespace DB
{

WatermarkSettingsPtr WatermarkSettings::clone() const
{
    auto result = std::make_shared<WatermarkSettings>(*this);
    if (result->expression)
        result->expression = result->expression->clone();

    return result;
}

StreamSettingsPtr StreamSettings::clone() const
{
    auto result = std::make_shared<StreamSettings>(*this);
    if (result->cursor)
        result->cursor = result->cursor->clone();
    if (result->watermark)
        result->watermark = result->watermark->clone();

    return result;
}

}
