#pragma once

#include <string>
#include <IO/WriteBuffer.h>


/// Displays the passed size in bytes as 123.45 GiB.
void formatReadableSizeWithBinarySuffix(double value, DB::WriteBuffer & out, int precision = 2);
std::string formatReadableSizeWithBinarySuffix(double value, int precision = 2);

/// Displays the passed size in bytes as 132.55 GB.
void formatReadableSizeWithDecimalSuffix(double value, DB::WriteBuffer & out, int precision = 2);
std::string formatReadableSizeWithDecimalSuffix(double value, int precision = 2);

/// Prints the number as 123.45 billion.
void formatReadableQuantity(double value, DB::WriteBuffer & out, int precision = 2);
std::string formatReadableQuantity(double value, int precision = 2);
