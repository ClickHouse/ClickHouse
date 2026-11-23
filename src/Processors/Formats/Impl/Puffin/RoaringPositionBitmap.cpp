#include <Processors/Formats/Impl/Puffin/RoaringPositionBitmap.h>

namespace DB
{

roaring::Roaring readRoaringPortableSafe(const char * data, size_t size, Int32 key)
{
    roaring::Roaring bitmap;
    try
    {
        bitmap = roaring::Roaring::readSafe(data, size);
    }
    catch (const std::exception & e)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failed to deserialize deletion vector roaring bitmap at key {}: {}", key, e.what());
    }

    /// `readSafe` only checks byte bounds: a structurally invalid bitmap can crash iteration or yield wrong positions.
    const char * reason = nullptr;
    if (!bitmap.internal_validate(&reason))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Failed to deserialize deletion vector roaring bitmap at key {}: {}",
            key,
            reason ? reason : "invalid bitmap structure");

    return bitmap;
}

}
