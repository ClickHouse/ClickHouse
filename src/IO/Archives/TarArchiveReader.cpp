#include "TarArchiveReader.h"
#include <IO/ReadBufferFromFileBase.h>
#include <Common/quoteString.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_UNPACK_ARCHIVE;
    extern const int LOGICAL_ERROR;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}
class TarArchiveReader::Handle
{
public:
    Handle(const String & path_to_archive_) : path_to_archive(path_to_archive_)
    {
        archive = archive_read_new();
        archive_read_support_filter_all(archive);
        archive_read_support_format_all(archive);
        if (archive_read_open_filename(archive, path_to_archive.c_str(), 10240) != ARCHIVE_OK)
        {
            throw Exception(ErrorCodes::CANNOT_UNPACK_ARCHIVE, "Couldn't open tar archive {}", quoteString(path_to_archive));
        }
        entry = archive_entry_new();
    }
    ~Handle()
    {
        archive_read_close(archive);
        archive_read_free(archive);
    }

    bool locateFile(const String & filename)
    {
        while (archive_read_next_header(archive, &entry) == ARCHIVE_OK)
        {
            if (archive_entry_pathname(entry) == filename)
                return true;
        }
        return false;
    }

    struct archive * archive;
    struct archive_entry * entry;

private:
    const String path_to_archive;
};

class TarArchiveReader::ReadBufferFromTarArchive : public ReadBufferFromFileBase
{
public:
    explicit ReadBufferFromTarArchive(const String & path_to_archive_, const String & filename_)
        : ReadBufferFromFileBase(DBMS_DEFAULT_BUFFER_SIZE, nullptr, 0)
        , handle(path_to_archive_)
        , path_to_archive(path_to_archive_)
        , filename(filename_)
    {
        handle.locateFile(filename_);
    }

    off_t seek(off_t off, int whence) override
    {
        off_t current_pos = getPosition();
        off_t new_pos;
        if (whence == SEEK_SET)
            new_pos = off;
        else if (whence == SEEK_CUR)
            new_pos = off + current_pos;
        else
            throw Exception(ErrorCodes::SEEK_POSITION_OUT_OF_BOUND, "Only SEEK_SET and SEEK_CUR seek modes allowed.");

        if (new_pos == current_pos)
            return current_pos; /// The position is the same.

        if (new_pos < 0)
            throw Exception(ErrorCodes::SEEK_POSITION_OUT_OF_BOUND, "Seek position is out of bound");

        off_t working_buffer_start_pos = current_pos - offset();
        off_t working_buffer_end_pos = current_pos + available();

        if ((working_buffer_start_pos <= new_pos) && (new_pos <= working_buffer_end_pos))
        {
            /// The new position is still inside the buffer.
            position() += new_pos - current_pos;
            return new_pos;
        }

        /// Check that the new position is now beyond the end of the file.
        if (new_pos > archive_entry_size(handle.entry))
            throw Exception(ErrorCodes::SEEK_POSITION_OUT_OF_BOUND, "Seek position is out of bound");

        ignore(new_pos - current_pos);
        return new_pos;
    }

    off_t getPosition() override { return archive_entry_size(handle.entry) - available(); }

    String getFileName() const override { return filename; }


private:
    bool nextImpl() override
    {
        auto bytes_read = archive_read_data(handle.archive, internal_buffer.begin(), static_cast<int>(internal_buffer.size()));

        if (!bytes_read)
            return false;

        working_buffer = internal_buffer;
        working_buffer.resize(bytes_read);
        return true;
    }
    Handle handle;
    const String path_to_archive;
    const String filename;
};

TarArchiveReader::TarArchiveReader(const String & path_to_archive_) : path_to_archive(path_to_archive_)
{
}

TarArchiveReader::TarArchiveReader(
    const String & path_to_archive_, const ReadArchiveFunction & archive_read_function_, UInt64 archive_size_)
    : path_to_archive(path_to_archive_), archive_read_function(archive_read_function_), archive_size(archive_size_)
{
}

TarArchiveReader::~TarArchiveReader()
{
}
bool TarArchiveReader::fileExists(const String & filename)
{
    Handle handle(path_to_archive);
    return handle.locateFile(filename);
}

TarArchiveReader::FileInfo TarArchiveReader::getFileInfo(const String & filename)
{
    Handle handle(path_to_archive);

    handle.locateFile(filename);
    FileInfo info;
    info.uncompressed_size = archive_entry_size(handle.entry);
    info.compressed_size = archive_entry_size(handle.entry);
    info.is_encrypted = false;

    return info;
}

std::unique_ptr<TarArchiveReader::FileEnumerator> TarArchiveReader::firstFile()
{
    return nullptr;
}

std::unique_ptr<ReadBufferFromFileBase> TarArchiveReader::readFile(const String & filename)
{
    Handle handle(path_to_archive);
    handle.locateFile(filename);

    return std::make_unique<ReadBufferFromTarArchive>(path_to_archive, filename);
}

std::unique_ptr<ReadBufferFromFileBase> TarArchiveReader::readFile([[maybe_unused]] std::unique_ptr<FileEnumerator> enumerator)
{
    return nullptr;
}

std::unique_ptr<TarArchiveReader::FileEnumerator> TarArchiveReader::nextFile([[maybe_unused]] std::unique_ptr<ReadBuffer> read_buffer)
{
    return nullptr;
}


void TarArchiveReader::setPassword([[maybe_unused]] const String & password_)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not set password to .tar archive");
}

}
