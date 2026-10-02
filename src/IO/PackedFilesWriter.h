#pragma once

#include <Common/PODArray.h>
#include <IO/PackedFilesIO.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteSettings.h>

namespace DB
{

/** Class that allows to write several files in "packed"
  * format into one data file (archive). Like the "tar" format
  * or similar, but much simpler. It buffers in memory all files
  * and writes them into one archive at @finalize method.
  * Before finalization all data is stored RAM.
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
    using OutBufferPtr = std::unique_ptr<WriteBufferFromFileBase>;
    using CommitDataFunc = std::function<void(String serialized_data, const WriteSettings & settings, bool need_sync)>;

    /// Creates memory buffer for the data of file
    /// and returns fake WriteBufferFromFileBase.
    OutBufferPtr writeFile(const String & file_name);

    /// The same as above, but also updated settings to write file with archive.
    OutBufferPtr writeFile(const String & file_name, const WriteSettings & settings);

    /// Common operations with files which modify only @files map.
    void moveFile(const String & from_name, const String & to_name);
    void replaceFile(const String & from_name, const String & to_name);

    void removeFile(const String & name);
    void removeFileIfExists(const String & name);

    bool isWritten(const String & name) const { return written_files.contains(name); }
    bool hasModifiedFiles() const { return !written_files.empty() || !metadata_changes.empty(); }

    /// Calculates index for written files.
    /// Dumps index and contents of files
    /// into the provided output write buffer.
    /// Returns calculated index of written files.
    /// The caller can provide files order hint to optimize the order of files in the archive. The files listed in the hint
    /// Will be written first in the archive in the specified order, and the rest of the files will be written after them.
    PackedFilesIO::Index finalize(CommitDataFunc commit_func, const Strings & files_order_hint = {});
    /// Returns a pair of (packed files index, need to fsync the archive)
    std::pair<PackedFilesIO::Index, bool> finalize(WriteBuffer & out, const Strings & files_order_hint = {});

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

    static void writePackedIndex(WriteBuffer & out, const PackedFilesIO::Index & index);

private:
    static size_t getSizeOfHeader();

    using Chars = PaddedPODArray<UInt8>;

    struct Data
    {
        Chars chars;
        bool need_sync = false;
    };

    /// Buffer that pretends to be WriteBufferFromFileBase but actually
    /// writes all data into provided array and keeps it in memory.
    class FakeWriteBufferFromFile : public WriteBufferFromFileBase
    {
    public:
        using Impl = WriteBufferFromVectorImpl<Chars>;

        FakeWriteBufferFromFile(const String & file_name_, std::shared_ptr<Data> data_)
            : WriteBufferFromFileBase(0, nullptr, 0)
            , file_name(file_name_)
            , data(data_)
            , impl(std::make_unique<Impl>(data->chars))
        {
            swap(*impl);
        }

        ~FakeWriteBufferFromFile() override
        {
            swap(*impl);
        }

        void nextImpl() override;
        void finalizeImpl() override;
        void cancelImpl() noexcept override;

        void sync() override { data->need_sync = true; }
        std::string getFileName() const override { return file_name; }

    private:

        struct SwapGuard
        {
            explicit SwapGuard(FakeWriteBufferFromFile & buf_) : buf(buf_)
            {
                buf.swap(*buf.impl);
            }

            ~SwapGuard()
            {
                buf.swap(*buf.impl);
            }

        private:
            FakeWriteBufferFromFile & buf;
        };

        const String file_name;
        /// We have shared_ptr here, because FakeWriteBufferFromFile
        /// can live longer than data stored in map of PackedFilesWriter.
        /// So shared_ptr here is just to avoid heap-use-after-free.
        std::shared_ptr<Data> data;
        const std::unique_ptr<Impl> impl;
    };

    template <typename Map>
    void applyMoveFile(MetadataChange & change, Map & index_map);

    template <typename Map>
    void applyRemoveFile(MetadataChange & change, Map & index_map);

    /// Map from the name of file to its content.
    std::map<String, std::shared_ptr<Data>> written_files;

    /// Changes of metadata such as file renames or removes.
    std::vector<MetadataChange> metadata_changes;

    /// Settings that are used while flushing archive with data.
    std::optional<WriteSettings> write_settings;
};

}
