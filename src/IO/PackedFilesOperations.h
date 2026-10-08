#pragma once
#include <Core/Types.h>
#include <IO/PackedFilesIO.h>

#include <Common/MapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

#include <filesystem>

namespace DB
{

class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

struct PackedFileInfo
{
    String name;
    String archive_name;
    PackedFilesIO::FileOffset offset;
};

using ArchiveListing = VectorWithMemoryTracking<PackedFileInfo>;

namespace fs = std::filesystem;

/// Lists files in packed archive in the order they are stored in the archive (i.e. sorted by offset).
ArchiveListing listPacked(const DiskPtr & disk_in, const String & input_file);
/// Lists files in packed archives in input_directory and all subdirectories. Also lists regular files as is.
/// All returned paths are relative to @base_input_dir.
MapWithMemoryTracking<fs::path, ArchiveListing> listPackedRecursive(const DiskPtr & disk_in, const String & base_input_dir, const String & sub_dir = "");
/// Prints listing to stdout. All paths are prefixed with @dir.
void printListing(const String & dir, const ArchiveListing & listing, std::ostream & out);

/// Extracts packed archive from @input_file into @output_dir.
void extractPacked(const DiskPtr & disk_in, const String & input_file, const DiskPtr & disk_out, const String & output_dir);

/// Recursively extracts packed archives from input_dir into output_dir.
/// It traverses input directory and subdirectories, unpacks files with
/// '.packed' extension to the same directory, copies other files as is.
void extractPackedRecursive(const DiskPtr & disk_in, const String & input_dir, const DiskPtr & disk_out, const String & output_dir);

/// Creates packed archive from @input_dir into @output_file
void createPacked(const DiskPtr & disk_in, const String & input_dir, const DiskPtr & disk_out, const String & output_file, Strings file_order_hint);

/// Recursively creates packed archives from @input_dir into @output_dir.
/// It traverses input directory and subdirectories, collects all files
/// in directory and writes them into packed archive with name 'data.packed'
void createPackedRecursive(const DiskPtr & disk_in, const String & input_dir, const DiskPtr & disk_out, const String & output_dir, Strings file_order_hint);

}
