#pragma once

#include <Processors/Formats/Impl/ExcelCommon.h>

#include <memory>
#include <string_view>


namespace DB::Excel
{

/// Opens a worksheet of a workbook in the `XLS` format (the binary format of Excel 97-2003, BIFF8, and of Excel 5.0/95, BIFF5).
/// `sheet` is the name or the 1-based index of the sheet; if it is empty, the first visible worksheet is opened.
std::unique_ptr<ISheetReader> openXLSSheet(std::string_view data, const String & sheet);

}
