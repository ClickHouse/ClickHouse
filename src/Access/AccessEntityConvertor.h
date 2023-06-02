#pragma once

#include <memory>
#include <Access/Common/AccessEntityType.h>
#include <MetadataACLEntry.pb.h>

namespace DB
{
struct IAccessEntity;

namespace FoundationDB
{
    Proto::AccessEntity toProto(const IAccessEntity & entity);
    std::shared_ptr<const IAccessEntity> fromProto(const Proto::AccessEntity & proto);
}
}
