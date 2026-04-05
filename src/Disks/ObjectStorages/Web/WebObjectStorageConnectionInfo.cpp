
#include <Disks/ObjectStorages/Web/WebObjectStorageConnectionInfo.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Disks/ObjectStorages/StoredObject.h>
#include <Disks/IO/ReadBufferFromWebServer.h>
#include <filesystem>
#include <Interpreters/Context.h>


namespace DB
{

struct WebObjectStorageConnectionInfo : public IObjectStorageConnectionInfo
{
    explicit WebObjectStorageConnectionInfo(const std::string & url_) : url(url_) {}

    explicit WebObjectStorageConnectionInfo(ReadBuffer & in)
    {
        DB::readBinary(url, in);
    }

    ObjectStorageType getType() const override { return ObjectStorageType::Web; }

    bool equals(const IObjectStorageConnectionInfo & other) const override
    {
        const auto * web_info = dynamic_cast<const WebObjectStorageConnectionInfo *>(&other);
        if (web_info)
            return url == web_info->url;
        return false;
    }

    void updateHash(SipHash & hash, bool /* include_credentials */) const override
    {
        hash.update(url);
    }

    void writeBinaryImpl(size_t /*mutual_protocol_version*/, WriteBuffer & out) override
    {
        DB::writeBinary(url, out);
    }

    std::unique_ptr<ReadBufferFromFileBase> createReader(
        const StoredObject & object,
        const ReadSettings & read_settings) override
    {
         return std::make_unique<ReadBufferFromWebServer>(
             std::filesystem::path(url) / object.remote_path,
             Context::getGlobalContextInstance(),
             object.bytes_size,
             read_settings,
             /* use_external_buffer */true,
             /* read_until_position */0);
    }

private:
    std::string url;
};

ObjectStorageConnectionInfoPtr getWebObjectStorageConnectionInfo(const std::string & url)
{
    return std::make_shared<WebObjectStorageConnectionInfo>(url);
}

ObjectStorageConnectionInfoPtr getWebObjectStorageConnectionInfo(ReadBuffer & in)
{
    return std::make_shared<WebObjectStorageConnectionInfo>(in);
}

}
