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

bool WatermarkSettings::operator==(const WatermarkSettings & rhs) const
{
    if (column != rhs.column)
        return false;

    if (idle_timeout != rhs.idle_timeout)
        return false;

    if ((expression == nullptr) != (rhs.expression == nullptr))
        return false;

    return !expression || expression->getTreeHash(/*ignore_aliases=*/false) == rhs.expression->getTreeHash(/*ignore_aliases=*/false);
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

bool StreamSettings::operator==(const StreamSettings & rhs) const
{
    /// Compare cursors
    {
        if ((cursor == nullptr) != (rhs.cursor == nullptr))
            return false;

        if (cursor)
            if (cursorTreeToMap(cursor) != cursorTreeToMap(rhs.cursor))
                return false;
    }

    /// Compare watermarks
    {
        if ((watermark == nullptr) != (rhs.watermark == nullptr))
            return false;

        if (watermark)
            if (*watermark != *rhs.watermark)
                return false;
    }

    return true;
}

}
