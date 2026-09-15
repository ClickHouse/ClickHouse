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

/// Returns the name with the sequence number set to `sequence_number`.
/// If the name already contains a number in this scheme, it is replaced:
/// `data.Parquet` -> `data.1.Parquet`, `data.5.Parquet` -> `data.1.Parquet`.
std::string setSequenceNumberInFileName(const std::string & path, size_t sequence_number);

/// Returns the number to continue the numbering from. If the name already contains a number
/// in this scheme, the numbering continues from the next one, which allows to start the numbering
/// from an arbitrary offset: for `data.5.Parquet` the next file is `data.6.Parquet`.
size_t getStartSequenceNumber(const std::string & path, size_t default_number);

/// The names of the files that an insert is written into when it is split into several files
/// (see `*_split_on_write_by_size_bytes` and `*_create_new_file_on_insert`).
struct NumberedFileNames
{
    /// Returns the name of the file with the given sequence number.
    std::function<std::string(size_t)> getName;
    /// The number to start the numbering from, see `getStartSequenceNumber`.
    size_t start_sequence_number = 1;
};

/// The numbering of a plain path, with the number placed into its name: `data.Parquet` -> `data.1.Parquet`, `data.2.Parquet`, ...
NumberedFileNames getNumberedFileNames(const std::string & path);

}
