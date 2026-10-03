#pragma once

#include <Common/MapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include <IO/PackedFilesIO.h>
#include <IO/SpillableMemoryWriteBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteSettings.h>

namespace DB
{

/** Class that allows to write several files in "packed"
  * format into one data file (archive). Like the "tar" format
  * or similar, but much simpler. It buffers in memory all files
  * and writes them into one archive at @finalize method.
  * The data of the files is kept in a shared SpillableMemoryWriteBuffer:
  * once its capacity (passed as `max_memory_size`) is exceeded, buffered data
  * is spilled to temporary files on the disk. @finalize streams
  * the archive into the provided buffer, so it does not need
  * a second copy of the whole archive in memory.
  * Each file is written continuously to avoid fragmentation
  * and large number of seeks while reading from remote filesystem.
  *
  * Format of file:
  * - Version of format - 1 byte.
  * - Number of files - 8 bytes.
  * - Index
  * - Data of written files
  *
  * Index has the following data for each file:
  * - Name of file.
  * - Offset in archive to the begin of file.
  * - Size of the file.
  */
class PackedFilesWriter
{
public:
    static constexpr size_t DEFAULT_MAX_MEMORY_SIZE = 32 * 1024 * 1024;

    using SpillConfig = SpillableMemoryWriteBuffer::SpillConfig;

    using OutBufferPtr = std::unique_ptr<WriteBufferFromFileBase>;

    /// All data is kept in memory (no spilling).
    PackedFilesWriter() = default;

    /// Buffered data spills to the disk once `spill_config->checker`'s capacity is
    /// exceeded; the spill files are created lazily in the temp directory managed by
    /// the config. The temp directory is removed on destruction.
    explicit PackedFilesWriter(std::shared_ptr<SpillConfig> spill_config_);

    ~PackedFilesWriter();

    /// Creates a SpillableMemoryWriteBuffer for the data of the file and returns it
    /// wrapped into a FakeWriteBufferFromFile.
    OutBufferPtr writeFile(const String & file_name);

    /// The same as above, but also updated settings to write file with archive.
    OutBufferPtr writeFile(const String & file_name, const WriteSettings & settings);

    /// Common operations with files which modify only @files map.
    void moveFile(const String & from_name, const String & to_name);
    void replaceFile(const String & from_name, const String & to_name);

    void removeFile(const String & name);
    void removeFileIfExists(const String & name);

    /// Uncompressed size for an already-written file; persisted only when finalized as v1+.
    void setUncompressedSize(const String & file_name, UInt64 uncompressed_size);

    bool isWritten(const String & name) const { return written_files.contains(name); }
    bool hasModifiedFiles() const { return !written_files.empty() || !metadata_changes.empty(); }

    /// Everything @finalize needs to know in advance: the order of the files in the archive,
    /// their index, and whether the archive has to be fsynced.
    struct FinalizePlan
    {
        PackedFilesIO::Index index;
        Strings ordered_file_names;
        UInt8 version = 0;
        /// fsync the whole archive if any of its files was requested to be fsynced.
        bool need_sync = false;
    };

    /// Validates the queued metadata changes, chooses the order of the files in the archive and
    /// calculates their index. This is the only phase of finalization that can throw, and it does
    /// not write anything, so a caller that streams the archive into a freshly opened destination
    /// file can open that file after this succeeded and never leave a truncated archive behind.
    /// The caller can provide files order hint to optimize the order of files in the archive. The files listed in the hint
    /// Will be written first in the archive in the specified order, and the rest of the files will be written after them.
    FinalizePlan prepareFinalize(const Strings & files_order_hint, UInt8 version) const;

    /// Dumps the index and the contents of the files into the provided output write buffer
    /// according to @plan.
    void finalize(WriteBuffer & out, const FinalizePlan & plan) const;

    /// Convenience overload of the two calls above for callers that write into a buffer which is
    /// already open - for example, into a region of a larger file - where a throwing preparation
    /// cannot damage a destination file. Use @prepareFinalize when the destination file is opened
    /// for the archive itself.
    /// Returns a pair of (packed files index, need to fsync the archive)
    std::pair<PackedFilesIO::Index, bool> finalize(WriteBuffer & out, const Strings & files_order_hint, UInt8 version) const;

    /// Settings of the files written into the archive. The archive is a single file on disk,
    /// so the settings of its members apply to it. The caller needs them to create the
    /// destination buffer before @finalize starts writing into it.
    WriteSettings getWriteSettings() const { return write_settings.value_or(WriteSettings{}); }

    /// Applies changes of files metadata both to the @written_files and @index.
    void applyMetadataChanges(PackedFilesIO::Index & index);

    struct MetadataChange
    {
        enum Type
        {
            MOVE,
            REPLACE,
            REMOVE,
            REMOVE_IF_EXISTS,
        };

        MetadataChange(Type type_, const String from_, const String & to_)
            : type(type_), from(from_), to(to_)
        {
        }

        Type type;
        String from;
        String to;

        bool is_applied = false;
    };

    static void writePackedIndex(WriteBuffer & out, const PackedFilesIO::Index & index, UInt8 version);

private:
    static size_t getSizeOfHeader();

    struct WrittenFile
    {
        explicit WrittenFile(std::shared_ptr<SpillableMemoryWriteBuffer> buffer_)
            : buffer(std::move(buffer_))
        {
        }

        std::shared_ptr<SpillableMemoryWriteBuffer> buffer;
        bool need_sync = false;
        UInt64 uncompressed_size = 0;
    };

    /// WriteBuffer that pretends to be a WriteBufferFromFileBase but forwards all
    /// writes to a shared SpillableMemoryWriteBuffer.
    class FakeWriteBufferFromFile : public WriteBufferFromFileBase
    {
    public:
        explicit FakeWriteBufferFromFile(std::shared_ptr<WrittenFile> file_);

        ~FakeWriteBufferFromFile() override;

        void nextImpl() override;
        void finalizeImpl() override;
        void cancelImpl() noexcept override;

        void sync() override { file->need_sync = true; }
        std::string getFileName() const override { return file->buffer->getFileName(); }

    private:

        /// We have shared_ptr here, because FakeWriteBufferFromFile
        /// can live longer than data stored in map of PackedFilesWriter.
        /// So shared_ptr here is just to avoid heap-use-after-free.
        std::shared_ptr<WrittenFile> file;
    };

    template <typename Map>
    void applyMoveFile(MetadataChange & change, Map & index_map);

    template <typename Map>
    void applyRemoveFile(MetadataChange & change, Map & index_map);

    /// Map from the name of file to its content.
    MapWithMemoryTracking<String, std::shared_ptr<WrittenFile>> written_files;

    /// Changes of metadata such as file renames or removes.
    VectorWithMemoryTracking<MetadataChange> metadata_changes;

    /// Settings that are used while flushing archive with data.
    std::optional<WriteSettings> write_settings;

    std::shared_ptr<SpillConfig> spill_config;
};

}
