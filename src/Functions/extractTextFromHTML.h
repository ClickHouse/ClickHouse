#pragma once

#include <cstddef>


namespace DB
{

/// Extracts the text from HTML by the rules of the function `extractTextFromHTML`.
/// Writes at most `size` bytes to `dst` and returns the number of written bytes.
size_t extractTextFromHTML(const char * src, size_t size, char * dst);

}
