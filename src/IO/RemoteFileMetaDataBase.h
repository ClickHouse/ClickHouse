#pragma once
#include <memory>
#include <base/types.h>
#include <functional>
#include <unordered_map>
#include <boost/core/noncopyable.hpp>

namespace DB
{
class RemoteFileMetaDataBase
{
public:
    RemoteFileMetaDataBase() = default;
    RemoteFileMetaDataBase(const String & schema_,
            const String & cluster_,
            const String & remote_path_,
            size_t file_size_,
            UInt64 last_modification_timestamp_):
        schema(schema_)
        ,cluster(cluster_)
        ,remote_path(remote_path_)
        ,file_size(file_size_)
        ,last_modification_timestamp(last_modification_timestamp_)
    {
    }
    virtual ~RemoteFileMetaDataBase();
    virtual String getClassName() const = 0; //class name
    // methods for basic information
    inline String getSchema() const { return schema; }
    inline String getCluster() const { return cluster; }
    inline size_t getFileSize() const { return file_size; }
    inline String getRemotePath() const { return remote_path; }
    inline UInt64 getLastModificationTimestamp() const { return last_modification_timestamp; }

    // deserialize
    virtual bool fromString(const String &buf) = 0;
    // serialize
    virtual String toString() const = 0;

    // used for comparing two file meta datas are the same or not.
    virtual String getVersion() const = 0;
protected:
    String schema;
    String cluster;
    String remote_path;
    size_t file_size = 0;
    UInt64 last_modification_timestamp = 0;
};

using RemoteFileMetaDataBasePtr = std::shared_ptr<RemoteFileMetaDataBase>;

/*
 * How to register a subclass into the factory and use it ?
 * 1) define your own subclass derive from RemoteFileMetaDataBase. Notice! the getClassName() must be the same
 *    as your subclass name.
 * 2) in a .cpp file, call REGISTTER_REMOTE_FILE_META_DATA_CLASS(subclass),
 * 3) call RemoteFileMetaDataFactory::instance().createClass(subclass_name) where you want to make a new object
 */

class RemoteFileMetaDataFactory : private boost::noncopyable
{
public:
    using ClassCreator = std::function<RemoteFileMetaDataBasePtr()>;
    ~RemoteFileMetaDataFactory() = default;

    static RemoteFileMetaDataFactory & instance();
    RemoteFileMetaDataBasePtr createClass(const String & class_name);
    void registerClass(const String &class_name, ClassCreator creator);
protected:
    RemoteFileMetaDataFactory() = default;

private:
    std::unordered_map<String, ClassCreator> class_creators;
};

// this should be used in a .cpp file. All the subclasses will finish the registeration before the main()
#define REGISTTER_REMOTE_FILE_META_DATA_CLASS(meta_data_class) \
    class FileMetaDataFactory##meta_data_class{\
    public:\
        FileMetaDataFactory##meta_data_class(){\
            auto creator = []() -> RemoteFileMetaDataBasePtr { return std::make_shared<meta_data_class>(); };\
            RemoteFileMetaDataFactory::instance().registerClass(#meta_data_class, creator);\
        }\
    };\
    static FileMetaDataFactory##meta_data_class g_file_meta_data_factory_instance##meta_data_class;
}
