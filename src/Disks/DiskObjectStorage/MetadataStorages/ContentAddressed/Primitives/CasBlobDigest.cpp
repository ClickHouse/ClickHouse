#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasBlobDigest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEnumWireTableAsserts.h>

namespace DB::Cas
{

static_assert(casEnumTableCoversEnum<kBlobHashAlgoWords, BlobHashAlgo>());

std::string_view blobHashAlgoName(BlobHashAlgo algo)
{
    return kBlobHashAlgoWords.toWord(algo, "blobHashAlgoName");
}

uint64_t blobHashLenFor(BlobHashAlgo algo)
{
    switch (algo)
    {
        case BlobHashAlgo::CityHash128:
        case BlobHashAlgo::XXH3_128:
            return 16;
        case BlobHashAlgo::Sha256:
            return 32;
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "blobHashLenFor: unknown BlobHashAlgo {}", static_cast<int>(algo));
}

BlobHashAlgo parseBlobHashAlgo(std::string_view config_value)
{
    if (config_value == "cityhash128")
        return BlobHashAlgo::CityHash128;
    if (config_value == "xxh3-128")
        return BlobHashAlgo::XXH3_128;
    if (config_value == "sha256")
        return BlobHashAlgo::Sha256;

    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "parseBlobHashAlgo: unknown cas_blob_hash config value '{}' (expected one of "
        "cityhash128|xxh3-128|sha256)", config_value);
}

}
