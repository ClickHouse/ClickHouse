#pragma once

#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <base/types.h>

#include <charconv>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace Excel
{

struct SheetInfo
{
    String name;
    bool is_worksheet = true;
    bool is_visible = true;
};

/// Selects a sheet by the value of the `input_format_excel_sheet` setting: a sheet name, or a 1-based index among all sheets
/// if no sheet has this name. If the setting is empty, selects the first visible worksheet.
inline size_t selectSheet(const std::vector<SheetInfo> & sheets, const String & sheet)
{
    auto check_worksheet = [&](size_t index)
    {
        if (!sheets[index].is_worksheet)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The sheet '{}' is not a worksheet (it may be a chart or a macro sheet)", sheets[index].name);
        return index;
    };

    if (sheet.empty())
    {
        for (size_t i = 0; i < sheets.size(); ++i)
            if (sheets[i].is_worksheet && sheets[i].is_visible)
                return i;
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The workbook does not have visible worksheets");
    }

    for (size_t i = 0; i < sheets.size(); ++i)
        if (sheets[i].name == sheet)
            return check_worksheet(i);

    size_t index = 0;
    auto [ptr, ec] = std::from_chars(sheet.data(), sheet.data() + sheet.size(), index);
    if (ec == std::errc() && ptr == sheet.data() + sheet.size() && index >= 1 && index <= sheets.size())
        return check_worksheet(index - 1);

    String names;
    for (const auto & info : sheets)
    {
        if (!names.empty())
            names += ", ";
        names += "'" + info.name + "'";
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "The workbook does not have a sheet '{}'. Available sheets: {}", sheet, names);
}

}

}
