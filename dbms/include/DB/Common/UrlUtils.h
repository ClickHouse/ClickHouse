#pragma once

#include <DB/Common/StringView.h>

/** Extracts scheme from given url.
 *
 * If there is no valid scheme then
 * empty StringView will be returned.
 */
StringView getUrlScheme(const StringView& url);
