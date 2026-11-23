#include <WindowFunctions/Common/WindowFunctionSettings.h>

#include <Core/Settings.h>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 window_sliding_aggregation_min_frame_rows;
}

WindowFunctionSettings WindowFunctionSettings::create(const Settings & settings)
{
    return WindowFunctionSettings{
        .sliding_aggregation_min_frame_rows = settings[Setting::window_sliding_aggregation_min_frame_rows],
    };
}

}
