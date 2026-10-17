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

/// The position in the path where `addSequenceNumberToFileName` inserts the sequence number (together with a dot before it).
size_t getSequenceNumberPositionInFileName(const std::string & path);

/// The names of the files that an insert is written into when it is split into several files
/// (see `*_split_on_write_by_size_bytes` and `*_create_new_file_on_insert`).
/// The numbering always starts from 1.
struct NumberedFileNames
{
    /// Returns the name of the file with the given sequence number.
    std::function<std::string(size_t)> getName;

    /// The numbered names may be the names of the first files of other partitions written by the same path pattern:
    /// with `p_{_partition_id}.tsv`, the file `p_a.1.tsv` is both the first numbered file of the partition `a`
    /// and the first file of the partition `a.1`. Nothing tells them apart, so an insert is never split into such names.
    bool may_be_names_of_other_partitions = false;
};

/// The numbering of a plain path, with the number placed into its name: `data.Parquet` -> `data.1.Parquet`, `data.2.Parquet`, ...
NumberedFileNames getNumberedFileNames(const std::string & path);

}
