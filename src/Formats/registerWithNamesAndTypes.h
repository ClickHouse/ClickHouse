#pragma once

#include <string>
#include <functional>

namespace DB
{
class FormatFactory;
class Block;

using RegisterWithNamesAndTypesFunc = std::function<void(const std::string & format_name, bool with_names, bool with_types)>;
void registerWithNamesAndTypes(const std::string & base_format_name, RegisterWithNamesAndTypesFunc register_func);

void markFormatWithNamesAndTypesSupportsSamplingColumns(const std::string & base_format_name, FormatFactory & factory);

/// Returns true if the header column names (when `with_names`) or the data type names (when
/// `with_types`) are not guaranteed to be valid UTF-8. Text output formats that write these into the
/// header verbatim (through escaping that escapes control characters but does not validate UTF-8) -
/// for example `TSV`/`CSV`/`CustomSeparated` `*WithNames*` and `TSKV` - use this as a
/// `may_produce_raw_bytes` checker so the text framings reject or base64-encode the output for a name
/// that is not valid UTF-8 (see `FormatFactory::checkIfOutputFormatMayProduceRawBytes`). A name is
/// knowable from the header before any row is written, unlike the row values themselves.
bool headerNamesMayProduceRawBytes(const Block & header, bool with_names, bool with_types);

}
