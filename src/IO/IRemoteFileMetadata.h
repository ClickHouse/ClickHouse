#pragma once
#include <memory>
#include <base/types.h>
#include <functional>
#include <unordered_map>
#include <boost/core/noncopyable.hpp>

namespace DB
{
class IRemoteFileMetadata
{
public:
    IRemoteFileMetadata() = default;
    IRemoteFileMetadata(const String & remote_path_,
            size_t file_size_,
            UInt64 last_modification_timestamp_):
        remote_path(remote_path_)
        ,file_size(file_size_)
        ,last_modification_timestamp(last_modification_timestamp_)
    {
    }
    virtual ~IRemoteFileMetadata();
    virtual String getName() const = 0; //class name
    // methods for basic information
    inline size_t getFileSize() const { return file_size; }
    inline String getRemotePath() const { return remote_path; }
    inline UInt64 getLastModificationTimestamp() const { return last_modification_timestamp; }

    // deserialize
    virtual bool fromString(const String & buf) = 0;
    // serialize
    virtual String toString() const = 0;

    // used for comparing two file metadatas are the same or not.
    virtual String getVersion() const = 0;
protected:
    String remote_path;
    size_t file_size = 0;
    UInt64 last_modification_timestamp = 0;
};

using IRemoteFileMetadataPtr = std::shared_ptr<IRemoteFileMetadata>;

/*
 * How to register a subclass into the factory and use it ?
 * 1) define your own subclass derive from IRemoteFileMetadata. Notice! the getName() must be the same
 *    as your subclass name.
 * 2) in a .cpp file, call REGISTTER_REMOTE_FILE_META_DATA_CLASS(subclass),
 * 3) call RemoteFileMetadataFactory::instance().createClass(subclass_name) where you want to make a new object
 */

class RemoteFileMetadataFactory : private boost::noncopyable
{
public:
    using ClassCreator = std::function<IRemoteFileMetadataPtr()>;
    ~RemoteFileMetadataFactory() = default;

    static RemoteFileMetadataFactory & instance();
    IRemoteFileMetadataPtr get(const String & name);
    void registerClass(const String &name, ClassCreator creator);
protected:
    RemoteFileMetadataFactory() = default;

private:
    std::unordered_map<String, ClassCreator> class_creators;
};

// this should be used in a .cpp file. All the subclasses will finish the registeration before the main()
#define REGISTTER_REMOTE_FILE_META_DATA_CLASS(metadata_class) \
    class FileMetadataFactory##metadata_class{\
    public:\
        FileMetadataFactory##metadata_class(){\
            auto creator = []() -> IRemoteFileMetadataPtr { return std::make_shared<metadata_class>(); };\
            RemoteFileMetadataFactory::instance().registerClass(#metadata_class, creator);\
        }\
    };\
    static FileMetadataFactory##metadata_class g_file_metadata_factory_instance##metadata_class;
}
