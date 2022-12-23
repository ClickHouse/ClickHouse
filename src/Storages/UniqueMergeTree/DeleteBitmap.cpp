#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteIntText.h>
#include <IO/WriteSettings.h>
#include <Storages/UniqueMergeTree/DeleteBitmap.h>
#include <roaring/roaring.h>
#include <Common/filesystemHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int UNKNOWN_EXCEPTION;
}

DeleteBitmap::DeleteBitmap() = default;

DeleteBitmap::DeleteBitmap(UInt64 version_) : version(version_)
{
    data = std::make_shared<RoaringBitmap>();
}

DeleteBitmap::DeleteBitmap(UInt64 version_, const std::vector<UInt32> & dels)
{
    version = version_;
    data = std::make_unique<RoaringBitmap>(dels.size(), dels.data());
}

DeleteBitmap::DeleteBitmap(const DeleteBitmap & rhs)
{
    version = rhs.version;
    data = std::make_shared<RoaringBitmap>(*rhs.data);
}

DeleteBitmap & DeleteBitmap::operator=(const DeleteBitmap & rhs)
{
    if (&rhs == this)
        return *this;
    version = rhs.version;
    data = std::make_shared<RoaringBitmap>(*rhs.data);
    return *this;
}

void DeleteBitmap::addDels(const std::vector<UInt32> & dels)
{
    if (!data)
    {
        data = std::make_shared<RoaringBitmap>(dels.size(), dels.data());
    }
    else
    {
        data->addMany(dels.size(), dels.data());
    }
}

UInt32 DeleteBitmap::rangeCardinality(size_t range_start, size_t range_end)
{
    if (data)
        return static_cast<UInt32>(
            roaring_bitmap_range_cardinality(reinterpret_cast<const roaring_bitmap_t *>(data.get()), range_start, range_end));
    return -1;
}

std::shared_ptr<DeleteBitmap> DeleteBitmap::addDelsAsNewVersion(UInt64 version_, const std::vector<UInt32> & dels)
{
    auto res = std::make_shared<DeleteBitmap>(*this);
    res->version = version_;
    res->addDels(dels);

    return res;
}

size_t DeleteBitmap::cardinality() const
{
    if (!data)
        return 0;
    return data->cardinality();
}

void DeleteBitmap::serialize(MutableDataPartStoragePtr data_part_storage) const
{
    if (!data_part_storage->exists("deletes"))
    {
        data_part_storage->createProjection("deletes");
    }
    else if (!data_part_storage->isDirectory("deletes"))
    {
        data_part_storage->removeFile("deletes");
        data_part_storage->createProjection("deletes");
    }
    const String file_path = "deletes/" + toString(version) + ".bitmap";
	try
	{
        auto out = data_part_storage->writeFile(file_path, DBMS_DEFAULT_BUFFER_SIZE, {});

        writeVarUInt(version, *out);

        size_t size = data->getSizeInBytes();
        writeVarUInt(size, *out);

        std::unique_ptr<char[]> buf(new char[size]);
        data->write(buf.get());
        out->write(buf.get(), size);
    }

	catch (...)
	{
        throw Exception(
            ErrorCodes::UNKNOWN_EXCEPTION,
            "Exception happend while serialize bitmap {}, version: {}, bitmap size: {}",
            data_part_storage->getFullRootPath() + file_path,
            version,
            data->getSizeInBytes());
    }
}

void DeleteBitmap::deserialize(MutableDataPartStoragePtr data_part_storage)
{
    const String file_path = "deletes/" + toString(version) + ".bitmap";
    if (!data_part_storage->exists(file_path))
    {
        throw Exception(
            ErrorCodes::FILE_DOESNT_EXIST, "Delete bitmap file {} does not exist.", data_part_storage->getFullRootPath() + file_path);
    }
    size_t size = 0;
    try
    {
        auto in = data_part_storage->readFile(file_path, {}, std::nullopt, std::nullopt);

        readVarUInt(version, *in);
        readVarUInt(size, *in);

        std::unique_ptr<char[]> buf(new char[size]);
        in->readStrict(buf.get(), size);
        data = std::make_shared<RoaringBitmap>(RoaringBitmap::read(buf.get()));
    }
    catch (...)
    {
        throw Exception(
            ErrorCodes::UNKNOWN_EXCEPTION,
            "Exception happend while deserialize bitmap {}, version: {}, bitmap size: {}",
            data_part_storage->getFullRootPath() + file_path,
            version,
            size);
    }
}
}
