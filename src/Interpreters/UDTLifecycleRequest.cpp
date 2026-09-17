#include <Interpreters/UDTLifecycleRequest.h>

#include <Parsers/ASTAlterTypeCommentQuery.h>
#include <Parsers/ASTCreateTypeQuery.h>
#include <Parsers/ASTDescribeTypeQuery.h>
#include <Parsers/ASTDropTypeQuery.h>
#include <Parsers/ASTPhysicalizeTypeReferencesQuery.h>
#include <Parsers/ASTRenameTypeQuery.h>
#include <Parsers/ASTShowCreateTypeQuery.h>
#include <Parsers/ASTShowTypesQuery.h>
#include <Common/Exception.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace DB::UDT
{

namespace
{
[[noreturn]] void unexpectedQuery()
{
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected AST at the user-defined type lifecycle boundary");
}
}

LifecycleRequestDescriptor classifyLifecycleRequest(const IAST & query)
{
    if (const auto * create = query.as<ASTCreateTypeQuery>())
    {
        return {
            .kind = create->attach ? LifecycleQueryKind::Attach : LifecycleQueryKind::Create,
            .required_access = AccessType::CREATE_TYPE,
            .mutation = true,
            .requires_internal_query = create->attach,
            .operation = create->attach ? "ATTACH TYPE" : "CREATE TYPE",
        };
    }
    if (query.as<ASTRenameTypeQuery>())
        return {LifecycleQueryKind::Rename, AccessType::ALTER_TYPE, true, false, "ALTER TYPE RENAME"};
    if (query.as<ASTAlterTypeCommentQuery>())
        return {LifecycleQueryKind::Comment, AccessType::ALTER_TYPE, true, false, "ALTER TYPE COMMENT"};
    if (query.as<ASTDropTypeQuery>())
        return {LifecycleQueryKind::DropRestrict, AccessType::DROP_TYPE, true, false, "DROP TYPE RESTRICT"};
    if (query.as<ASTShowTypesQuery>())
        return {LifecycleQueryKind::ShowTypes, AccessType::SHOW_TYPES, false, false, "SHOW TYPES"};
    if (query.as<ASTShowCreateTypeQuery>())
        return {LifecycleQueryKind::ShowCreate, AccessType::SHOW_TYPES, false, false, "SHOW CREATE TYPE"};
    if (query.as<ASTDescribeTypeQuery>())
        return {LifecycleQueryKind::Describe, AccessType::SHOW_TYPES, false, false, "DESCRIBE TYPE"};
    if (query.as<ASTPhysicalizeTypeReferencesQuery>())
    {
        return {
            LifecycleQueryKind::DeferredPhysicalization,
            AccessType::NONE,
            false,
            false,
            "PHYSICALIZE TYPE REFERENCES",
        };
    }
    if (query.as<ASTApplyPhysicalizeTypeReferencesQuery>())
    {
        return {
            LifecycleQueryKind::DeferredPhysicalization,
            AccessType::NONE,
            true,
            false,
            "PHYSICALIZE TYPE REFERENCES APPLY",
        };
    }
    unexpectedQuery();
}

String getLifecycleRequestDatabase(const IAST & query)
{
    if (const auto * create = query.as<ASTCreateTypeQuery>())
        return create->getDatabase();
    if (const auto * rename = query.as<ASTRenameTypeQuery>())
        return rename->getDatabase();
    if (const auto * comment = query.as<ASTAlterTypeCommentQuery>())
        return comment->getDatabase();
    if (const auto * drop = query.as<ASTDropTypeQuery>())
        return drop->getDatabase();
    if (const auto * show = query.as<ASTShowTypesQuery>())
        return show->getDatabase();
    if (const auto * show_create = query.as<ASTShowCreateTypeQuery>())
        return show_create->getDatabase();
    if (const auto * describe = query.as<ASTDescribeTypeQuery>())
        return describe->getDatabase();
    if (const auto * physicalize = query.as<ASTPhysicalizeTypeReferencesQuery>())
        return physicalize->getDatabase();
    if (query.as<ASTApplyPhysicalizeTypeReferencesQuery>())
        return {};
    unexpectedQuery();
}

String getLifecycleRequestLocalName(const IAST & query)
{
    if (const auto * create = query.as<ASTCreateTypeQuery>())
        return create->getTypeName();
    if (const auto * rename = query.as<ASTRenameTypeQuery>())
        return rename->getTypeName();
    if (const auto * comment = query.as<ASTAlterTypeCommentQuery>())
        return comment->getTypeName();
    if (const auto * drop = query.as<ASTDropTypeQuery>())
        return drop->getTypeName();
    if (const auto * show_create = query.as<ASTShowCreateTypeQuery>())
        return show_create->getTypeName();
    if (const auto * describe = query.as<ASTDescribeTypeQuery>())
        return describe->getTypeName();
    if (query.as<ASTShowTypesQuery>() || query.as<ASTPhysicalizeTypeReferencesQuery>()
        || query.as<ASTApplyPhysicalizeTypeReferencesQuery>())
        return {};
    unexpectedQuery();
}

String getLifecycleRequestCluster(const IAST & query)
{
    if (const auto * create = query.as<ASTCreateTypeQuery>())
        return create->cluster;
    if (const auto * rename = query.as<ASTRenameTypeQuery>())
        return rename->cluster;
    if (const auto * comment = query.as<ASTAlterTypeCommentQuery>())
        return comment->cluster;
    if (const auto * drop = query.as<ASTDropTypeQuery>())
        return drop->cluster;
    if (const auto * physicalize = query.as<ASTPhysicalizeTypeReferencesQuery>())
        return physicalize->cluster;
    if (query.as<ASTShowTypesQuery>() || query.as<ASTShowCreateTypeQuery>() || query.as<ASTDescribeTypeQuery>()
        || query.as<ASTApplyPhysicalizeTypeReferencesQuery>())
        return {};
    unexpectedQuery();
}

}
