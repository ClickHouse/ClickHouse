#pragma once

#include <string>
#include <common/types.h>

#define UNICODE_BAR_CHAR_SIZE (strlen("█"))


/** Allows you to draw a unicode-art bar whose width is displayed with a resolution of 1/8 character.
  */
namespace UnicodeBar
{
    double getWidth(double x, double min, double max, double max_width);
    size_t getWidthInBytes(double width);

    /// In `dst` there must be a space for barWidthInBytes(width) characters and a trailing zero.
    void render(double width, char * dst);
    std::string render(double width);
}
