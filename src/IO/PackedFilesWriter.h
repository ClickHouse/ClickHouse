#pragma once

#include <Common/VectorWithMemoryTracking.h>
#include <Common/MapWithMemoryTracking.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/PackedFilesIO.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteSettings.h>

#include <boost/noncopyable.hpp>

#include <functional>

namespace DB
{

class ReadBuffer;

/// Temporary file for the data that PackedFilesWriter moves out of memory.
/// All ranges of data are written first, and then each range is read back separately.
/// The implementation is on top of the temporary data of the server (see `TemporaryDataBuffer`),
/// which is not available at the level of IO.
class IPackedFilesTemporaryFile
{
public:
    virtual ~IPackedFilesTemporaryFile() = default;

    /// Starts a new range of data and returns the position to read it from.
    virtual size_t startRange() = 0;
    virtual void write(const char * data, size_t size) = 0;
    /// Must be called after all ranges are written and before reading them. Can be called several times.
    virtual void finishWriting() = 0;

    /// Returns a buffer reading the data from the beginning of the range at `position` returned by `startRange`.
    virtual std::unique_ptr<ReadBuffer> readRange(size_t position) = 0;
};

using PackedFilesTemporaryFilePtr = std::unique_ptr<IPackedFilesTemporaryFile>;

/** Class that allows to write several files in "packed"
  * format into one data file (archive). Like the "tar" format
  * or similar, but much simpler. It buffers in memory all files
  * and writes them into one archive at @finalize method.
  * By default all data is stored in RAM before finalization.
  * With @setSpillSettings the memory is bounded: once the buffered data
  * exceeds the limit, the data of the largest files is moved
  * to a temporary file and read back by @finalize.
  * @finalize streams the archive into the provided buffer, so it does not need
  * a second copy of the whole archive in memory.
  * Each file is written continuously to avoid fragmentation
  * and large number of seeks while reading from remote filesystem.
  * The writer is not thread-safe, including the buffers returned by @writeFile.
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
class PackedFilesWriter : private boost::noncopyable
{
public:
    using OutBufferPtr = std::unique_ptr<WriteBufferFromFileBase>;

    PackedFilesWriter();
    ~PackedFilesWriter();

    struct SpillSettings
    {
        /// Creates the temporary file for the data moved out of memory, on the first move.
        std::function<PackedFilesTemporaryFilePtr()> create_temporary_file;
        /// Maximum amount of memory taken by the buffered data of the files. 0 means unlimited.
        size_t max_bytes_in_memory = 0;
    };

    /// Bounds the memory taken by the buffered data of the files: once it exceeds
    /// `max_bytes_in_memory`, the data of the largest files is moved to a temporary file.
    void setSpillSettings(SpillSettings spill_settings_);

    /// Creates memory buffer for the data of file
    /// and returns fake WriteBufferFromFileBase.
    /// Throws if the file is already written.
    OutBufferPtr writeFile(const String & file_name);

    /// The same as above, but also updated settings to write file with archive.
    OutBufferPtr writeFile(const String & file_name, const WriteSettings & settings);

    /// The same as above, but replaces the file if it is already written, like rewriting a file on a disk.
    OutBufferPtr rewriteFile(const String & file_name, const WriteSettings & settings);

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

    /// Sizes of the memory chunks of a file: they grow exponentially, so that small files take little memory.
    /// The last chunk of a file that is still being written cannot be moved out of memory, so the chunks
    /// are kept small: otherwise a part with many files being written at once (e.g. a wide part with many columns)
    /// would take much more memory than `max_bytes_in_memory`.
    static constexpr size_t INITIAL_CHUNK_SIZE = 256;
    static constexpr size_t MAX_CHUNK_SIZE = 64 * 1024;

    /// State shared between the writer and the data of its files. The data of a file
    /// can outlive the writer (it is referenced by FakeWriteBufferFromFile).
    struct SharedState
    {
        /// Allocated size of the memory chunks of all files.
        size_t bytes_in_memory = 0;
        /// Allocated size of the last chunks of the files that are still being written: they cannot be moved out of memory.
        size_t non_spillable_bytes_in_memory = 0;
        /// Reset by the destructor of the writer.
        PackedFilesWriter * writer = nullptr;
    };

    /// A range of the data of a file that was moved to the temporary file.
    struct RangeInTemporaryFile
    {
        /// Position returned by IPackedFilesTemporaryFile::startRange.
        size_t position = 0;
        size_t size = 0;
    };

    struct Data
    {
        explicit Data(std::shared_ptr<SharedState> state_);
        ~Data();

        void addChunk();
        /// Size of the data written into the chunk with the given index.
        size_t getChunkDataSize(size_t chunk_index) const;
        /// Returns the number of the first chunks that can be moved out of memory:
        /// the last chunk of a file that is still being written is used by its write buffer.
        size_t getNumSpillableChunks() const;
        /// Frees the first `num_chunks` chunks.
        void removeChunks(size_t num_chunks);

        std::shared_ptr<SharedState> state;

        /// The data of the file is its ranges in the temporary file followed by its chunks, in order.
        VectorWithMemoryTracking<RangeInTemporaryFile> ranges_in_temporary_file;
        /// All chunks except the last one are filled entirely.
        VectorWithMemoryTracking<Memory<>> chunks;
        size_t next_chunk_size = INITIAL_CHUNK_SIZE;

        /// The fields below are set when the file is finalized.
        bool is_finalized = false;
        size_t last_chunk_data_size = 0;
        UInt64 size = 0;

        bool need_sync = false;
        UInt64 uncompressed_size = 0;
    };

    /// Buffer that pretends to be WriteBufferFromFileBase but actually
    /// writes all data into the memory chunks of the file.
    class FakeWriteBufferFromFile : public WriteBufferFromFileBase
    {
    public:
        FakeWriteBufferFromFile(const String & file_name_, std::shared_ptr<Data> data_);

        void nextImpl() override;
        void finalizeImpl() override;

        void sync() override { data->need_sync = true; }
        std::string getFileName() const override { return file_name; }

    private:
        const String file_name;
        /// We have shared_ptr here, because FakeWriteBufferFromFile
        /// can live longer than data stored in map of PackedFilesWriter.
        /// So shared_ptr here is just to avoid heap-use-after-free.
        std::shared_ptr<Data> data;
    };

    template <typename Map>
    void applyMoveFile(MetadataChange & change, Map & index_map);

    template <typename Map>
    void applyRemoveFile(MetadataChange & change, Map & index_map);

    /// Called when the memory taken by the files grows.
    void spillIfNeeded();
    void spill();

    std::shared_ptr<SharedState> state;
    SpillSettings spill_settings;
    /// Temporary file with the data moved out of memory by all calls of @spill. Created on the first one.
    PackedFilesTemporaryFilePtr temporary_file;

    /// Map from the name of file to its content.
    MapWithMemoryTracking<String, std::shared_ptr<Data>> written_files;

    /// Changes of metadata such as file renames or removes.
    VectorWithMemoryTracking<MetadataChange> metadata_changes;

    /// Settings that are used while flushing archive with data.
    std::optional<WriteSettings> write_settings;
};

}
