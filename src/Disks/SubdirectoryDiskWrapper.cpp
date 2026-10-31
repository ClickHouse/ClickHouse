#include <Disks/SubdirectoryDiskWrapper.h>
#include <Disks/DirectoryIterator.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

class SubdirectoryDiskWrapperDirectoryIterator final : public IDirectoryIterator
{
public:
    SubdirectoryDiskWrapperDirectoryIterator(DirectoryIteratorPtr delegate_, String path_prefix_)
        : delegate(std::move(delegate_))
        , path_prefix(std::move(path_prefix_))
    {
    }

    void next() override { delegate->next(); }
    bool isValid() const override { return delegate->isValid(); }

    String path() const override
    {
        auto delegate_path = delegate->path();

        if (!delegate_path.starts_with(path_prefix))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Disk iterator returned path {} without expected prefix {}",
                quoteString(delegate_path),
                quoteString(path_prefix));

        return delegate_path.substr(path_prefix.size());
    }

    String name() const override { return delegate->name(); }

private:
    DirectoryIteratorPtr delegate;
    String path_prefix;
};

}

SubdirectoryDiskWrapper::SubdirectoryDiskWrapper(const DiskPtr & delegate_, const String & sub_path_)
    : IDisk(delegate_->getName())
    , delegate(delegate_)
    , sub_path(sub_path_)
{
    if (sub_path.empty() || sub_path.starts_with('/') || sub_path.ends_with('/'))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Subdirectory {} of disk {} must be a non-empty relative path without leading and trailing slashes",
            quoteString(sub_path), delegate->getName());

    path = delegate->getPath();
    if (!path.empty() && !path.ends_with('/'))
        path += '/';
    path += sub_path + '/';
}

String SubdirectoryDiskWrapper::wrap(const String & file_path) const
{
    /// The paths are joined as strings, not as `fs::path`, so that an absolute `file_path`
    /// stays inside `sub_path` instead of replacing it.
    if (file_path.empty())
        return sub_path;
    if (file_path.starts_with('/'))
        return sub_path + file_path;
    return sub_path + '/' + file_path;
}

DirectoryIteratorPtr SubdirectoryDiskWrapper::iterateDirectory(const String & dir_path) const
{
    return std::make_unique<SubdirectoryDiskWrapperDirectoryIterator>(delegate->iterateDirectory(wrap(dir_path)), sub_path + '/');
}

std::unique_ptr<ReadBufferFromFileBase> SubdirectoryDiskWrapper::readFileIfExists(
    const String & file_path,
    const ReadSettings & settings,
    std::optional<size_t> read_hint) const
{
    return delegate->readFileIfExists(wrap(file_path), settings, read_hint);
}

std::unique_ptr<WriteBufferFromFileBase> SubdirectoryDiskWrapper::writeFile(
    const String & file_path,
    size_t buf_size,
    WriteMode mode,
    const WriteSettings & settings)
{
    return delegate->writeFile(wrap(file_path), buf_size, mode, settings);
}

ReservationPtr SubdirectoryDiskWrapper::reserve(UInt64)
{
    /// A reservation refers to the delegate disk, which addresses files outside of `sub_path`.
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Reservations are not supported for a subdirectory of disk {}", getName());
}

ReservationPtr SubdirectoryDiskWrapper::reserve(UInt64, const ReservationConstraints &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Reservations are not supported for a subdirectory of disk {}", getName());
}

}
