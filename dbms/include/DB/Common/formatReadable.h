#pragma once

#include <string>


/// Выводит переданный размер в байтах в виде 123.45 GiB.
std::string formatReadableSizeWithBinarySuffix(double value, int precision = 2);

/// Выводит переданный размер в байтах в виде 132.55 GB.
std::string formatReadableSizeWithDecimalSuffix(double value, int precision = 2);

/// Выводит число в виде 123.45 billion.
std::string formatReadableQuantity(double value, int precision = 2);
