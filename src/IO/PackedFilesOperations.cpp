#include <IO/PackedFilesOperations.h>
#include <IO/PackedFilesReader.h>
#include <IO/PackedFilesWriter.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/copyData.h>
#include <IO/WriteHelpers.h>
#include <Disks/IDisk.h>

#include <ostream>

namespace DB
{

namespace ErrorCodes
{
    extern const int DIRECTORY_DOESNT_EXIST;
    extern const int INCORRECT_DATA;
}

static std::string_view normalizePath(std::string_view path)
{
    if (!path.empty() && path.ends_with('/'))
        return path.substr(0, path.size() - 1);
    return path;
}

/// Reject a recursive operation whose output directory is the same as, or nested under, the input
/// directory on the same backing store. Otherwise the directory walk would descend into the files it
/// just generated, copying output into itself indefinitely.
static void assertOutputNotInsideInput(
    const DiskPtr & disk_in, std::string_view input_dir, const DiskPtr & disk_out, std::string_view output_dir)
{
    /// The recursive walk descends into whatever the write step produces, so the hazard exists whenever
    /// the output directory is the same as, or nested under, the input directory on the same backing
    /// store. A disk name is not a reliable identity for "same backing store": clickhouse-disks always
    /// exposes both `default` (rooted at the server path) and `local` (rooted at `/`), so the same
    /// filesystem tree is addressable under two different disk names.
    ///
    /// For local disks getPath() is the absolute filesystem root, so rooting each disk-relative operand
    /// there makes the two names resolve to the same absolute path and lets a plain containment check
    /// catch the overlap (including two local disks whose roots nest, e.g. `/` and `/var/lib/...`).
    /// Remote disks have no comparable filesystem root, so restrict them to same-disk-object identity.
    fs::path in;
    fs::path out;
    if (!disk_in->isRemote() && !disk_out->isRemote())
    {
        /// A local disk root is not guaranteed absolute: `clickhouse disks` roots `default`/`local` at
        /// absolute paths, but the standalone clickhouse-packed-io builds DiskLocal("./"), so a relative
        /// root mixed with an absolute operand (e.g. `-i /abs/part -o ./out`) would leave one side
        /// relative and the other absolute, and lexically_relative returns empty for such a pair. Anchor
        /// both against the current directory so they are comparable and reflect where files actually land.
        in = fs::absolute(fs::path(disk_in->getPath()) / normalizePath(input_dir)).lexically_normal();
        out = fs::absolute(fs::path(disk_out->getPath()) / normalizePath(output_dir)).lexically_normal();
    }
    else
    {
        if (disk_in != disk_out)
            return;

        /// Root both paths before comparing so a disk-relative "." (the disk root / current directory)
        /// is treated as the root it names. Comparing raw normalized strings would reduce "." vs "./out"
        /// to "." vs "out" and miss that the output is nested under the input.
        in = (fs::path("/") / normalizePath(input_dir)).lexically_normal();
        out = (fs::path("/") / normalizePath(output_dir)).lexically_normal();
    }

    /// The output is the same as, or nested under, the input iff the path from input to output does not
    /// step outside it (i.e. does not start with "..").
    auto relative = out.lexically_relative(in);
    if (!relative.empty() && *relative.begin() != "..")
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Output path {} is the same as or nested under the input path {} on the same backing store; "
            "the recursive operation would descend into its own generated output",
            output_dir, input_dir);
}

ArchiveListing listPacked(const DiskPtr & disk_in, const String & input_file)
{
    PackedFilesReader reader(disk_in, input_file, getReadSettings());
    const auto & index = reader.getIndex();
    const fs::path archive_path(input_file);
    VectorWithMemoryTracking<PackedFileInfo> files;
    for (const auto & [name, offset] : index)
        files.push_back({name, archive_path.filename(), offset});
    /// Sort by offset to make the order as in the archive.
    std::sort(files.begin(), files.end(),
        [](const auto & a, const auto & b) { return a.offset.offset < b.offset.offset; });
    return files;
}

MapWithMemoryTracking<fs::path, ArchiveListing> listPackedRecursive(const DiskPtr & disk_in, const String & base_input_dir, const String & sub_dir)
{
    auto sub_dir_path = fs::path(sub_dir);
    auto input_dir = base_input_dir / sub_dir_path;
    if (!disk_in->existsDirectory(input_dir))
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Input path {} doesn't exist or is not a directory", input_dir);

    MapWithMemoryTracking<fs::path, ArchiveListing> listing;
    for (auto it = disk_in->iterateDirectory(input_dir); it->isValid(); it->next())
    {
        auto in_path = input_dir / it->name();
        if (disk_in->existsDirectory(in_path))
        {
            auto subtree_listing = listPackedRecursive(disk_in, base_input_dir, sub_dir_path / it->name());
            for (auto & [subtree_path, subtree_files] : subtree_listing)
                listing[subtree_path] = subtree_files;
        }
        else if (in_path.extension() == PackedFilesIO::ARCHIVE_EXTENSION)
        {
            auto & current_listing = listing[sub_dir_path];
            auto more_files = listPacked(disk_in, in_path);
            current_listing.insert(current_listing.end(), more_files.begin(), more_files.end());
        }
        else
        {
            auto & current_listing = listing[sub_dir_path];
            auto size = disk_in->getFileSize(in_path);
            current_listing.push_back({it->name(), "", PackedFilesIO::FileOffset{0, size}});
        }
    }

    return listing;
}

void printListing(const String & dir, const ArchiveListing & listing, std::ostream & out)
{
    String path_prefix = dir.empty() ? "" : dir + "/";
    for (const auto & file : listing)
        out << path_prefix << file.name << "\t"
            << (file.archive_name.empty() ? String("-") : path_prefix + file.archive_name) << "\t"
            << (file.archive_name.empty() ? String("-") : toString(file.offset.offset)) << "\t"
            << file.offset.size << '\n';
}

void extractPacked(const DiskPtr & disk_in, const String & input_file, const DiskPtr & disk_out, const String & output_dir)
{
    if (!disk_out->existsDirectory(output_dir))
        disk_out->createDirectories(output_dir);

    PackedFilesReader reader(disk_in, input_file, getReadSettings());
    auto files = reader.getFileNames();

    for (const auto & file_name : files)
    {
        /// File names come from the archive index, so a crafted archive could contain an absolute path
        /// or `..` components to escape output_dir. Reject those before writing anything to disk_out.
        const fs::path entry_path(file_name);
        if (entry_path.is_absolute())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Packed archive entry has an absolute path: {}", file_name);
        for (const auto & component : entry_path)
            if (component == "..")
                throw Exception(ErrorCodes::INCORRECT_DATA, "Packed archive entry escapes the output directory: {}", file_name);

        auto in = reader.readFile(disk_in, input_file, file_name, {}, {});
        auto out = disk_out->writeFile(output_dir / entry_path);
        copyData(*in, *out);
        out->finalize();
    }
}

void extractPackedRecursive(const DiskPtr & disk_in, const String & input_dir, const DiskPtr & disk_out, const String & output_dir)
{
    if (!disk_in->existsDirectory(input_dir))
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Input directory {} doesn't exist", input_dir);

    assertOutputNotInsideInput(disk_in, input_dir, disk_out, output_dir);

    auto input_dir_path = fs::path(normalizePath(input_dir));
    auto output_dir_path = output_dir / input_dir_path.filename();

    if (!disk_out->existsDirectory(output_dir_path))
        disk_out->createDirectories(output_dir_path);

    for (auto it = disk_in->iterateDirectory(input_dir); it->isValid(); it->next())
    {
        auto in_path = input_dir_path / fs::path(it->name());

        if (disk_in->existsDirectory(in_path))
            extractPackedRecursive(disk_in, in_path, disk_out, output_dir_path);
        else if (in_path.extension() == PackedFilesIO::ARCHIVE_EXTENSION)
            extractPacked(disk_in, in_path, disk_out, output_dir_path);
        else
            disk_in->copyFile(in_path, *disk_out, output_dir_path / it->name(), getReadSettings());
    }
}

void createPacked(const DiskPtr & disk_in, const String & input_dir, const DiskPtr & disk_out, const String & output_file, Strings file_order_hint)
{
    if (!disk_in->existsDirectory(input_dir))
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Input path {} doesn't exist or is not a directory", input_dir);

    auto parent_path = parentPath(output_file);

    /// The output archive must not land inside the directory being packed. createPacked walks input_dir
    /// one level deep, so if the archive's own directory is input_dir (or nested under it) a re-run would
    /// read the previously written archive as an input member and nest a stale data.packed inside the new
    /// one. Reject that (the recursive entrypoint already guarantees the output is outside the input).
    assertOutputNotInsideInput(disk_in, input_dir, disk_out, parent_path);

    if (!disk_out->existsDirectory(parent_path))
        disk_out->createDirectories(parent_path);

    PackedFilesWriter writer;
    auto read_settings = getReadSettings();

    for (auto it = disk_in->iterateDirectory(input_dir); it->isValid(); it->next())
    {
        auto in_path = input_dir / fs::path(it->name());
        if (disk_in->existsDirectory(in_path))
            continue;

        auto in = disk_in->readFile(in_path, read_settings);
        auto out = writer.writeFile(it->name());
        copyData(*in, *out);
        out->finalize();
    }

    writer.finalize(
        [&](String serialised_data, const WriteSettings & settings, bool need_sync)
        {
            auto buf =  disk_out->writeFile(output_file, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, settings);
            buf->write(serialised_data.data(), serialised_data.size());
            buf->finalize();
            if (need_sync)
                buf->sync();
        },
        file_order_hint,
        PackedFilesIO::VERSION_WITHOUT_UNCOMPRESSED_SIZE);
}

void createPackedRecursive(const DiskPtr & disk_in, const String & input_dir, const DiskPtr & disk_out, const String & output_dir, Strings file_order_hint)
{
    assertOutputNotInsideInput(disk_in, input_dir, disk_out, output_dir);

    auto input_dir_path = fs::path(normalizePath(input_dir));
    auto output_dir_path = output_dir / input_dir_path.filename();

    if (!disk_out->existsDirectory(output_dir_path))
        disk_out->createDirectories(output_dir_path);

    auto output_file = fs::path(output_dir_path) / (String("data") + PackedFilesIO::ARCHIVE_EXTENSION);
    createPacked(disk_in, input_dir, disk_out, output_file, file_order_hint);

    for (auto it = disk_in->iterateDirectory(input_dir); it->isValid(); it->next())
    {
        auto in_path = input_dir / fs::path(it->name());
        if (!disk_in->existsDirectory(in_path))
            continue;

        String sub_dir_prefix = it->name() +"/";
        Strings files_order_hint_for_subdir;
        /// Filter files_order_hint to only include files from the current subdirectory and cut off the prefix
        for (const auto & file : file_order_hint)
            if (file.starts_with(sub_dir_prefix))
                files_order_hint_for_subdir.push_back(file.substr(sub_dir_prefix.size()));
        createPackedRecursive(disk_in, in_path, disk_out, output_dir_path, files_order_hint_for_subdir);
    }
}

}
