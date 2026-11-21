#pragma once

#include "config.h"

#if USE_MINIZIP

#include <Processors/Formats/Impl/ExcelCommon.h>

#include <memory>
#include <string_view>


namespace DB::Excel
{

/// Opens a worksheet of a workbook in the `XLSX` (Office Open XML) format.
/// `data` is the whole file, it must stay alive while the sheet is read.
/// `sheet` is the name or the 1-based index of the sheet; if it is empty, the first visible worksheet is opened.
std::unique_ptr<ISheetReader> openXLSXSheet(std::string_view data, const String & sheet);

}

#endif
