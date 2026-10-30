#pragma once

#include <Backups/BackupIO_Default.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPHeaderEntries.h>
#include <Interpreters/Context_fwd.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/URI.h>


namespace DB
{

/// Represents a backup stored at HTTP(S) URL.
class BackupReaderURL : public BackupReaderDefault
{
public:
    BackupReaderURL(
        const Poco::URI & uri_,
        const String & http_user_name,
        const String & http_password,
        const ReadSettings & read_settings_,
        const WriteSettings & write_settings_,
        const ContextPtr & context_);

    ~BackupReaderURL() override;

    bool fileExists(const String & file_name) override;
    UInt64 getFileSize(const String & file_name) override;

    std::unique_ptr<ReadBufferFromFileBase> readFile(const String & file_name) override;

private:
    Poco::URI getURIForFile(const String & file_name) const;

    const Poco::URI base_uri;
    const Poco::Net::HTTPBasicCredentials credentials;
    const ConnectionTimeouts timeouts;
    const size_t max_redirects;
    const ContextPtr context;
};


class BackupWriterURL : public BackupWriterDefault
{
public:
    BackupWriterURL(
        const Poco::URI & uri_,
        const String & http_user_name,
        const String & http_password,
        const String & http_method_,
        const ReadSettings & read_settings_,
        const WriteSettings & write_settings_,
        const ContextPtr & context_);

    ~BackupWriterURL() override;

    bool fileExists(const String & file_name) override;
    UInt64 getFileSize(const String & file_name) override;
    std::unique_ptr<WriteBuffer> writeFile(const String & file_name) override;

    void copyFile(const String & destination, const String & source, size_t size) override;
    void removeFile(const String & file_name) override;

private:
    std::unique_ptr<ReadBuffer> readFile(const String & file_name, size_t expected_file_size) override;
    Poco::URI getURIForFile(const String & file_name) const;

    const Poco::URI base_uri;
    const Poco::Net::HTTPBasicCredentials credentials;
    const String http_method;
    const ConnectionTimeouts timeouts;
    const size_t max_redirects;
    const ContextPtr context;
};

}
