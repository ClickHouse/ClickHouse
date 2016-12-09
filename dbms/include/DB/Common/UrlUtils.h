#pragma once

#include <DB/Common/StringView.h>

/// Extracts scheme from given url.
StringView getUrlScheme(const StringView& url);


/// Extracts host from given url.
StringView getUrlHost(const StringView& url);
