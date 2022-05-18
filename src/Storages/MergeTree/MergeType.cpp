#include <Storages/MergeTree/MergeType.h>
#include <base/EnumReflection.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

MergeType checkAndGetMergeType(UInt64 merge_type)
{
    if (auto maybe_merge_type = magic_enum::enum_cast<MergeType>(merge_type))
        return *maybe_merge_type;

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unknown MergeType {}", static_cast<UInt64>(merge_type));
}

bool isTTLMergeType(MergeType merge_type)
{
    return merge_type == MergeType::TTLDelete || merge_type == MergeType::TTLRecompress;
}

}
