#pragma once

#include <Access/Common/AccessType.h>
#include <Core/Types.h>

#include <string_view>

namespace DB
{
class IAST;
}

namespace DB::UDT
{

enum class LifecycleQueryKind : UInt8
{
    Create = 1,
    Attach = 2,
    Rename = 3,
    DropRestrict = 4,
    ShowTypes = 5,
    ShowCreate = 6,
    Describe = 7,
    DeferredPhysicalization = 8,
    Comment = 9,
};

struct LifecycleRequestDescriptor
{
    LifecycleQueryKind kind{};
    AccessType required_access = AccessType::NONE;
    bool mutation = false;
    bool requires_internal_query = false;
    std::string_view operation;
};

/// Classifies only the ASTs routed to InterpreterUDTQuery. It has
/// no Context/catalog/storage callbacks and is suitable for ordering-policy
/// tests. An unrelated AST is a caller bug and throws LOGICAL_ERROR.
LifecycleRequestDescriptor classifyLifecycleRequest(const IAST & query);

String getLifecycleRequestDatabase(const IAST & query);
String getLifecycleRequestLocalName(const IAST & query);
String getLifecycleRequestCluster(const IAST & query);

}
