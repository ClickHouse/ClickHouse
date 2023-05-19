#include "DataPartMetadata.h"

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/IPartMetadataManager.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <base/JSON.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_TTL_FILE;
    extern const int LOGICAL_ERROR;
}

void DataPartMetadata::serialize(WriteBuffer & /*out*/, UInt16 /*metadata_format_version*/) const
{
    /// TODO
}

void DataPartMetadata::deserialize(ReadBuffer & /*in*/)
{
    *this = {};

    /// TODO
}

void DataPartMetadata::listPossibleOldStyleFiles(Strings & out) const
{
    out.push_back(UUID_FILE_NAME);
    out.push_back(COUNT_FILE_NAME);
    out.push_back(TTL_FILE_NAME);
    out.push_back(CHECKSUMS_FILE_NAME);
    out.push_back(DEFAULT_COMPRESSION_CODEC_FILE_NAME);
    out.push_back(METADATA_VERSION_FILE_NAME);
}

static void loadUUID(DataPartMetadata & meta, const IPartMetadataManager & manager)
{
    bool exists = manager.exists(DataPartMetadata::UUID_FILE_NAME);
    if (exists)
    {
        auto in = manager.read(DataPartMetadata::UUID_FILE_NAME);
        readText(meta.uuid, *in);
        if (meta.uuid == UUIDHelpers::Nil)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected empty {}", String(DataPartMetadata::UUID_FILE_NAME));
    }
}

static void loadTTLInfos(DataPartMetadata & meta, const IPartMetadataManager & manager)
{
    bool exists = manager.exists(DataPartMetadata::TTL_FILE_NAME);
    if (exists)
    {
        auto in = manager.read(DataPartMetadata::TTL_FILE_NAME);
        assertString("ttl format version: ", *in);
        size_t format_version;
        readText(format_version, *in);
        assertChar('\n', *in);

        if (format_version == 1)
        {
            try
            {
                meta.ttl_infos.read(*in);
            }
            catch (const JSONException &)
            {
                throw Exception(ErrorCodes::BAD_TTL_FILE, "Error while parsing file ttl.txt");
            }
        }
        else
            throw Exception(ErrorCodes::BAD_TTL_FILE, "Unknown ttl format version: {}", toString(format_version));
    }
}

void DataPartMetadata::loadFromOldStyleFiles(const IPartMetadataManager & manager, bool is_projection)
{
    loadUUID(*this, manager);
    if (!is_projection)
        loadTTLInfos(*this, manager);
}

}
