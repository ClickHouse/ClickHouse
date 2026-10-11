#pragma once

#include <cstddef>
#include <functional>
#include <string>

namespace DB
{

/// The parts of the data written into multiple files (see `*_create_new_file_on_insert`
/// and `*_split_on_write_by_size_bytes` settings) are named with a sequence number,
/// which is placed after the name of the file and before its extension:
/// `data.Parquet`, `data.1.Parquet`, `data.2.Parquet`, ...

/// Returns the name with the sequence number inserted before the first dot of the name of the file.
/// A number that is already in the name is not taken for a sequence number and is kept as is:
/// `data.Parquet` -> `data.1.Parquet`, `data.5.Parquet` -> `data.1.5.Parquet`. Otherwise a name such as
/// `export.2026.csv` would continue as `export.2027.csv` - the name of an unrelated file.
std::string addSequenceNumberToFileName(const std::string & path, size_t sequence_number);

/// The names of the files that an insert is written into when it is split into several files
/// (see `*_split_on_write_by_size_bytes` and `*_create_new_file_on_insert`).
/// The numbering always starts from 1.
struct NumberedFileNames
{
    /// Returns the name of the file with the given sequence number.
    std::function<std::string(size_t)> getName;
};

/// The numbering of a plain path, with the number placed into its name: `data.Parquet` -> `data.1.Parquet`, `data.2.Parquet`, ...
NumberedFileNames getNumberedFileNames(const std::string & path);

}
