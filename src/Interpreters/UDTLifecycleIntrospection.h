#pragma once

#include <DataTypes/UDT/Record.h>
#include <Databases/UDT/AtomicAuthorityStartupStatus.h>
#include <Parsers/IAST_fwd.h>

#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

namespace DB::UDT
{

class LifecycleIntrospectionError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidCanonicalSQL,
        RecordNameMismatch,
        RecordIdentityMismatch,
        RecordCommentMismatch,
        InvalidRecord,
    };

    LifecycleIntrospectionError(Code code_, std::string_view message);

    const Code code;
};

/// Returns pointers into `records`, ordered by qualified database/name. LIKE
/// applies to the local name with the ordinary case-sensitive SQL LIKE escape
/// rules. The caller keeps the owning authority snapshot alive.
std::vector<const Record *> selectRecordsForShow(
    std::span<const Record> records, std::optional<std::string_view> like_pattern = std::nullopt);

/// Parses the already-validated canonical authority SQL, clones it, converts
/// ATTACH to user-facing CREATE, and strips every internal replay field.
ASTPtr makeShowCreateTypeQuery(const Record & record);

using DescribeRows = std::vector<std::pair<String, String>>;

/// Defines the DESCRIBE TYPE public shape. Rows are returned in the
/// declared order as `property String, value String`; parameter/dependency
/// arrays use a stable one-line tuple representation.
DescribeRows makeDescribeTypeRows(
    std::string_view resolved_database_display_name,
    const Record & record,
    AuthorityDefinitionStatus status = AuthorityDefinitionStatus::Active);
}
