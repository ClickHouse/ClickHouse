#pragma once

#include <string>
#include <base/types.h>

namespace DB
{

/**
  * The FileRenamer class provides functionality for renaming files based on given pattern with placeholders
  * The supported placeholders are:
  *   %a - Full original file name ("sample.csv")
  *   %f - Original filename without extension ("sample")
  *   %e - Original file extension with dot (".csv")
  *   %t - Timestamp (in microseconds)
  *   %% - Percentage sign ("%")
  *
  * Example:
  *   Pattern             - "processed_%f_%t%e"
  *   Original filename   - "sample.csv"
  *   New filename        - "processed_sample_1683405960646224.csv"
  */
class FileRenamer
{
public:
    FileRenamer();

    explicit FileRenamer(const String & renaming_rule);

    String generateNewFilename(const String & filename) const;

    bool isEmpty() const;

    static bool validateRenamingRule(const String & rule, bool throw_on_error = false);

private:
    String rule;
};

} // DB
