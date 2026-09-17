#include <Interpreters/UDTLifecycleIntrospection.h>

#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTCreateTypeQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserCreateTypeQuery.h>
#include <Parsers/parseQuery.h>
#include <Common/DateLUT.h>
#include <Common/Exception.h>
#include <Common/likePatternToRegexp.h>
#include <Common/quoteString.h>
#include <Common/re2.h>

#include <algorithm>

namespace DB::UDT
{
namespace
{

constexpr std::size_t maximum_canonical_sql_bytes = 256ULL << 10;
constexpr std::size_t maximum_parser_depth = 256;
constexpr std::size_t maximum_parser_backtracks = 100'000;

[[noreturn]] void fail(LifecycleIntrospectionError::Code code, std::string_view message)
{
    throw LifecycleIntrospectionError(code, message);
}

String lowerHexDigest(const Digest & digest)
{
    static constexpr char digits[] = "0123456789abcdef";
    String result(digest.size() * 2, '\0');
    for (std::size_t index = 0; index < digest.size(); ++index)
    {
        const UInt8 byte = digest[index];
        result[2 * index] = digits[byte >> 4];
        result[2 * index + 1] = digits[byte & 0x0f];
    }
    return result;
}

std::string_view parameterKindName(ParameterKind kind)
{
    switch (kind)
    {
        case ParameterKind::Type: return "Type";
        case ParameterKind::Bool: return "Bool";
        case ParameterKind::UInt8: return "UInt8";
        case ParameterKind::UInt16: return "UInt16";
        case ParameterKind::UInt32: return "UInt32";
        case ParameterKind::UInt64: return "UInt64";
        case ParameterKind::Int8: return "Int8";
        case ParameterKind::Int16: return "Int16";
        case ParameterKind::Int32: return "Int32";
        case ParameterKind::Int64: return "Int64";
        case ParameterKind::String: return "String";
    }
    fail(LifecycleIntrospectionError::Code::InvalidRecord, "type record contains an unknown parameter kind");
}

String formatParameters(const Record & record)
{
    String result = "[";
    for (std::size_t index = 0; index < record.parameters.size(); ++index)
    {
        if (index)
            result += ", ";
        result += "(" + quoteString(record.parameters[index].normalized_name) + ", "
            + quoteString(parameterKindName(record.parameters[index].kind)) + ")";
    }
    result += "]";
    return result;
}

String formatDependencies(const Record & record)
{
    String result = "[";
    for (std::size_t index = 0; index < record.dependencies.size(); ++index)
    {
        if (index)
            result += ", ";
        const auto & dependency = record.dependencies[index];
        result += "(" + quoteString(toString(dependency.type_uuid)) + ", " + std::to_string(dependency.revision) + ", "
            + quoteString(lowerHexDigest(dependency.target_definition_hash)) + ")";
    }
    result += "]";
    return result;
}

String formatSemanticABIIdentity(const Record & record)
{
    return "checker_abi=" + std::to_string(record.checker_abi) + ";checker_charge_abi=" + std::to_string(record.checker_charge_abi)
        + ";policy_abi=" + std::to_string(record.policy_abi) + ";function_registry_abi=" + std::to_string(record.function_registry_abi);
}

String formatCreationTime(Int64 creation_time_us_utc)
{
    WriteBufferFromOwnString buffer;
    writeDateTimeText<'-', ':', 'T'>(DateTime64(creation_time_us_utc), 6, buffer, DateLUT::instance("UTC"));
    buffer.write('Z');
    return buffer.str();
}

std::string_view storageBackendName(StorageBackend backend)
{
    switch (backend)
    {
        case StorageBackend::AtomicDisk: return "AtomicDisk";
    }
    fail(LifecycleIntrospectionError::Code::InvalidRecord, "type record contains an unknown storage backend");
}

void validateParsedRecord(const ASTCreateTypeQuery & query, const Record & record)
{
    if (!query.attach || !query.database || query.getDatabase().empty() || !query.definition || query.if_not_exists
        || !query.cluster.empty())
        fail(
            LifecycleIntrospectionError::Code::InvalidCanonicalSQL,
            "canonical type SQL has execution-only or missing fields");
    if (query.getTypeName() != record.normalized_local_name || query.getDatabase() + "." + query.getTypeName() != record.normalized_name)
        fail(LifecycleIntrospectionError::Code::RecordNameMismatch, "canonical type SQL name disagrees with its record");

    if (!query.uuid || !query.revision || !query.definition_hash || *query.uuid != record.identity.type_uuid
        || *query.revision != record.identity.revision || *query.definition_hash != lowerHexDigest(record.definition_hash))
        fail(
            LifecycleIntrospectionError::Code::RecordIdentityMismatch,
            "canonical ATTACH TYPE identity disagrees with its record");

    const auto * comment = query.comment ? query.comment->as<ASTLiteral>() : nullptr;
    if ((!comment && !record.comment.empty())
        || (comment && (comment->value.getType() != Field::Types::String || comment->value.safeGet<String>() != record.comment)))
        fail(
            LifecycleIntrospectionError::Code::RecordCommentMismatch,
            "canonical type SQL comment disagrees with its record");
}

}

LifecycleIntrospectionError::LifecycleIntrospectionError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

std::vector<const Record *>
selectRecordsForShow(std::span<const Record> records, std::optional<std::string_view> like_pattern)
{
    std::optional<re2::RE2> matcher;
    if (like_pattern)
    {
        re2::RE2::Options options;
        options.set_log_errors(false);
        matcher.emplace(likePatternToRegexp(*like_pattern), options);
        if (!matcher->ok())
            fail(LifecycleIntrospectionError::Code::InvalidCanonicalSQL, "LIKE pattern did not compile");
    }

    std::vector<const Record *> result;
    result.reserve(records.size());
    for (const auto & record : records)
    {
        if (!matcher || re2::RE2::PartialMatch(record.normalized_local_name, *matcher))
            result.push_back(&record);
    }
    std::sort(
        result.begin(),
        result.end(),
        [](const auto * lhs, const auto * rhs)
        {
            if (lhs->normalized_name != rhs->normalized_name)
                return lhs->normalized_name < rhs->normalized_name;
            if (lhs->identity.type_uuid != rhs->identity.type_uuid)
                return lhs->identity.type_uuid < rhs->identity.type_uuid;
            return lhs->identity.revision < rhs->identity.revision;
        });
    return result;
}

ASTPtr makeShowCreateTypeQuery(const Record & record)
{
    try
    {
        ParserCreateTypeQuery parser;
        ASTPtr parsed = parseQuery(
            parser,
            record.canonical_definition_sql,
            "user-defined type SHOW CREATE record",
            maximum_canonical_sql_bytes,
            maximum_parser_depth,
            maximum_parser_backtracks);
        const auto * create = parsed ? parsed->as<ASTCreateTypeQuery>() : nullptr;
        if (!create)
            fail(LifecycleIntrospectionError::Code::InvalidCanonicalSQL, "canonical type SQL is not CREATE/ATTACH TYPE");
        validateParsedRecord(*create, record);

        ASTPtr result = parsed->clone();
        auto & shown = result->as<ASTCreateTypeQuery &>();
        shown.attach = false;
        shown.if_not_exists = false;
        shown.uuid.reset();
        shown.revision.reset();
        shown.definition_hash.reset();
        shown.cluster.clear();
        return result;
    }
    catch (const LifecycleIntrospectionError &)
    {
        throw;
    }
    catch (const Exception &)
    {
        fail(
            LifecycleIntrospectionError::Code::InvalidCanonicalSQL, "canonical type SQL cannot be parsed for SHOW CREATE");
    }
}

DescribeRows makeDescribeTypeRows(std::string_view resolved_database_display_name, const Record & record, AuthorityDefinitionStatus status)
{
    String decreases_parameter;
    if (record.decreasing_parameter)
    {
        if (*record.decreasing_parameter >= record.parameters.size())
            fail(LifecycleIntrospectionError::Code::InvalidRecord, "type record decreasing parameter is out of range");
        decreases_parameter = record.parameters[*record.decreasing_parameter].normalized_name;
    }

    return {
        {"database", String{resolved_database_display_name}},
        {"name", record.normalized_local_name},
        {"uuid", toString(record.identity.type_uuid)},
        {"revision", std::to_string(record.identity.revision)},
        {"underlying_type", record.canonical_physical_template_sql},
        {"definition_hash", lowerHexDigest(record.definition_hash)},
        {"semantic_abi_identity", formatSemanticABIIdentity(record)},
        {"checker_certificate_hash", lowerHexDigest(record.checker_certificate_digest)},
        {"parameters", formatParameters(record)},
        {"decreases_parameter", std::move(decreases_parameter)},
        {"dependencies", formatDependencies(record)},
        {"owner", record.owner_display_name},
        {"comment", record.comment},
        {"creation_time", formatCreationTime(record.creation_time_us_utc)},
        {"storage_backend", String{storageBackendName(record.storage_backend)}},
        {"status", String{getAuthorityDefinitionStatusName(status)}},
    };
}

}
