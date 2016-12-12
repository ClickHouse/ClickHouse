#pragma once

#include <DB/Common/StringView.h>

/// Percent decode of url data.
std::string decodeUrl(const StringView & url);


/// Extracts scheme from given url.
StringView getUrlScheme(const StringView & url);


/// Extracts host from given url.
StringView getUrlHost(const StringView & url);
