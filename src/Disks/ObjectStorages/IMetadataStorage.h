#pragma once

#include <memory>
#include <vector>
#include <unordered_map>
#include <Poco/Timestamp.h>
#include <IO/ReadSettings.h>
#include <IO/WriteSettings.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Disks/DirectoryIterator.h>
#include <Disks/WriteMode.h>

namespace DB
{

struct IMetadataOperation
{
    virtual void execute() = 0;
    virtual void undo() = 0;
    virtual void finalize() {}
    virtual ~IMetadataOperation() = default;
};

using MetadataOperationPtr = std::unique_ptr<IMetadataOperation>;

struct IMetadataTransaction : private boost::noncopyable
{
public:
    virtual void addOperation(MetadataOperationPtr && operation);
    virtual void commit() = 0;
    virtual void rollback() = 0;

    virtual ~IMetadataTransaction() = default;
};

using MetadataTransactionPtr = std::shared_ptr<IMetadataTransaction>;

class IMetadataStorage : private boost::noncopyable
{

public:
    virtual MetadataTransactionPtr createTransaction() const = 0;

    virtual const std::string & getPath() const = 0;
    virtual bool exists(const std::string & path) const = 0;
    virtual bool isFile(const std::string & path) const = 0;
    virtual bool isDirectory(const std::string & path) const = 0;
    virtual Poco::Timestamp getLastModified(const std::string & path) const = 0;

    virtual std::vector<std::string> listDirectory(const std::string & path) const = 0;
    virtual DirectoryIteratorPtr iterateDirectory(const String & path) = 0;


    virtual std::unique_ptr<ReadBufferFromFileBase> readFile(  /// NOLINT
         const std::string & path,
         const ReadSettings & settings = ReadSettings{},
         std::optional<size_t> read_hint = {},
         std::optional<size_t> file_size = {}) const;

    virtual std::unique_ptr<WriteBufferFromFileBase> writeFile( /// NOLINT
         const std::string & path,
         MetadataTransactionPtr transaction,
         size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
         const WriteSettings & settings = {});

    virtual void setLastModified(const std::string & path, const Poco::Timestamp & timestamp, MetadataTransactionPtr transaction) = 0;

    virtual void unlinkFile(const std::string & path, MetadataTransactionPtr transaction) = 0;

    virtual void createDirectory(const std::string & path, MetadataTransactionPtr transaction) = 0;

    virtual void createDicrectoryRecursive(const std::string & path, MetadataTransactionPtr transaction) = 0;

    virtual void removeDirectory(const std::string & path, MetadataTransactionPtr transaction) = 0;

    virtual void removeRecursive(const std::string & path, MetadataTransactionPtr transaction) = 0;

    virtual void createHardLink(const std::string & path_from, const std::string & path_to, MetadataTransactionPtr transaction) = 0;

    virtual void moveFile(const std::string & path_from, const std::string & path_to, MetadataTransactionPtr transaction) = 0;

    virtual void moveDirectory(const std::string & path_from, const std::string & path_to, MetadataTransactionPtr transaction) = 0;

    virtual void replaceFile(const std::string & path_from, const std::string & path_to, MetadataTransactionPtr transaction) = 0;

    virtual ~IMetadataStorage() = default;
};

using MetadataStoragePtr = std::shared_ptr<IMetadataStorage>;

}
