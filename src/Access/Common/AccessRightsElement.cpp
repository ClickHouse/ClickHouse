#include <Access/AccessControl.h>
#include <Access/Common/AccessRightsElement.h>
#include <Access/Common/AccessType.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Parsers/IAST.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_GRANT;
    extern const int LOGICAL_ERROR;
}

namespace
{
    void formatOptions(bool grant_option, bool is_partial_revoke, String & result)
    {
        if (is_partial_revoke)
        {
            if (grant_option)
                result.insert(0, "REVOKE GRANT OPTION ");
            else
                result.insert(0, "REVOKE ");
        }
        else
        {
            if (grant_option)
                result.insert(0, "GRANT ").append(" WITH GRANT OPTION");
            else
                result.insert(0, "GRANT ");
        }
    }

    void formatAccessFlagsWithColumns(const AccessRightsElement & element, String & result)
    {
        String columns_as_str;
        if (!element.anyColumn())
        {
            WriteBufferFromString buffer(columns_as_str);
            element.formatColumnNames(buffer);
        }

        auto keywords = element.access_flags.toKeywords();
        if (keywords.empty())
        {
            result += "USAGE";
            return;
        }

        bool need_comma = false;
        for (std::string_view keyword : keywords)
        {
            if (need_comma)
                result.append(", ");
            need_comma = true;
            result += keyword;
            result += columns_as_str;
        }
    }

    String toStringImpl(const AccessRightsElement & element, bool with_options)
    {
        String result;
        formatAccessFlagsWithColumns(element, result);
        result += " ";

        WriteBufferFromOwnString buffer;
        element.formatONClause(buffer);
        result += buffer.str();

        if (with_options)
            formatOptions(element.grant_option, element.is_partial_revoke, result);
        return result;
    }

    String toStringImpl(const AccessRightsElements & elements, bool with_options)
    {
        if (elements.empty())
            return with_options ? "GRANT USAGE ON *.*" : "USAGE ON *.*";

        String result;
        String part;

        for (size_t i = 0; i != elements.size(); ++i)
        {
            const auto & element = elements[i];

            if (!part.empty())
                part += ", ";
            formatAccessFlagsWithColumns(element, part);

            bool next_element_uses_same_table_and_options = false;
            if (i != elements.size() - 1)
            {
                const auto & next_element = elements[i + 1];
                if (element.sameDatabaseAndTableAndParameter(next_element) && element.sameOptions(next_element))
                {
                    next_element_uses_same_table_and_options = true;
                }
            }

            if (!next_element_uses_same_table_and_options)
            {
                part += " ";
                WriteBufferFromOwnString buffer;
                element.formatONClause(buffer);
                part += buffer.str();

                if (with_options)
                    formatOptions(element.grant_option, element.is_partial_revoke, part);
                if (result.empty())
                    result = std::move(part);
                else
                    result.append(", ").append(part);
                part.clear();
            }
        }

        return result;
    }
}

void AccessRightsElement::formatColumnNames(WriteBuffer & buffer) const
{
    buffer << "(";
    bool need_comma = false;
    for (const auto & column : columns)
    {
        if (std::exchange(need_comma, true))
            buffer << ", ";
        buffer << backQuoteIfNeed(column);
        if (wildcard)
            buffer << "*";
    }
    buffer << ")";
}

void AccessRightsElement::formatFilter(WriteBuffer & buffer) const
{
    buffer << "(" << backQuoteIfNeed(filter) << ")";
}

void AccessRightsElement::formatONClause(WriteBuffer & buffer) const
{
    auto is_enabled_user_name_access_type = true;
    auto is_enabled_read_write_grants = true;
    if (const auto context = Context::getGlobalContextInstance())
    {
        const auto & access_control = context->getAccessControl();
        is_enabled_user_name_access_type = access_control.isEnabledUserNameAccessType();
        is_enabled_read_write_grants = access_control.isEnabledReadWriteGrants();
    }

    buffer << "ON ";
    if (isGlobalWithParameter())
    {
        /// Special check for backward compatibility.
        /// If `enable_user_name_access_type` is set to false, we will dump `GRANT CREATE USER ON *` as `GRANT CREATE USER ON *.*`.
        /// This will allow us to run old replicas in the same cluster.
        if (access_flags.getParameterType() == AccessFlags::USER_NAME
            && !is_enabled_user_name_access_type)
        {
            if (!anyParameter())
                LOG_WARNING(getLogger("AccessRightsElement"),
                    "Converting {} to *.* because the setting `enable_user_name_access_type` is `false`. "
                    "Consider turning this setting on, if your cluster contains no replicas older than 25.1",
                    parameter);

            buffer << "*.*";
        }
        else
        {
            if (anyParameter())
                buffer << "*";
            else
            {
                buffer << backQuoteIfNeed(parameter);
                if (wildcard)
                    buffer << "*";
                else
                {
                    if (hasFilter() && is_enabled_read_write_grants)
                        formatFilter(buffer);
                }
            }
        }
    }
    else if (anyDatabase())
        buffer << "*.*";
    else if (!table.empty())
    {
        if (!database.empty())
            buffer << backQuoteIfNeed(database) << ".";

        buffer << backQuoteIfNeed(table);

        if (columns.empty() && wildcard)
            buffer << "*";
    }
    else
    {
        buffer << backQuoteIfNeed(database);

        if (wildcard)
            buffer << "*";

        buffer << ".*";
    }
}


AccessRightsElement::AccessRightsElement(AccessFlags access_flags_, std::string_view database_)
    : access_flags(access_flags_), database(database_), parameter(database_)
{
}

AccessRightsElement::AccessRightsElement(AccessFlags access_flags_, std::string_view database_, std::string_view table_)
    : access_flags(access_flags_), database(database_), table(table_)
{
}

AccessRightsElement::AccessRightsElement(
    AccessFlags access_flags_, std::string_view database_, std::string_view table_, std::string_view column_)
    : access_flags(access_flags_)
    , database(database_)
    , table(table_)
    , columns({String{column_}})
{
}

AccessRightsElement::AccessRightsElement(
    AccessFlags access_flags_,
    std::string_view database_,
    std::string_view table_,
    const std::vector<std::string_view> & columns_)
    : access_flags(access_flags_), database(database_), table(table_)
{
    columns.resize(columns_.size());
    for (size_t i = 0; i != columns_.size(); ++i)
        columns[i] = String{columns_[i]};
}

AccessRightsElement::AccessRightsElement(
    AccessFlags access_flags_, std::string_view database_, std::string_view table_, const Strings & columns_)
    : access_flags(access_flags_)
    , database(database_)
    , table(table_)
    , columns(columns_)
{
}

AccessFlags AccessRightsElement::getGrantableFlags() const
{
    if (isGlobalWithParameter() && !anyParameter())
        return access_flags & AccessFlags::allFlagsGrantableOnGlobalWithParameterLevel();
    else if (!anyColumn())
        return access_flags & AccessFlags::allFlagsGrantableOnColumnLevel();
    else if (!anyTable())
        return access_flags & AccessFlags::allFlagsGrantableOnTableLevel();
    else if (!anyDatabase())
        return access_flags & AccessFlags::allFlagsGrantableOnDatabaseLevel();
    else
        return access_flags & AccessFlags::allFlagsGrantableOnGlobalLevel();
}

void AccessRightsElement::throwIfNotGrantable() const
{
    if (empty())
        return;
    auto grantable_flags = getGrantableFlags();
    if (grantable_flags)
    {
        if (!anyColumn() && (anyTable() || anyDatabase()))
        {
            // Specifying specific columns with a wildcard for a database/table is grammatically valid, but not logically valid
            throw Exception(ErrorCodes::INVALID_GRANT, "{} on wildcards cannot be granted on the column level", access_flags.toString());
        }
        return;
    }

    if (!anyColumn())
        throw Exception(ErrorCodes::INVALID_GRANT, "{} cannot be granted on the column level", access_flags.toString());
    if (!anyTable())
        throw Exception(ErrorCodes::INVALID_GRANT, "{} cannot be granted on the table level", access_flags.toString());
    if (!anyDatabase())
        throw Exception(ErrorCodes::INVALID_GRANT, "{} cannot be granted on the database level", access_flags.toString());
    if (!anyParameter())
        throw Exception(ErrorCodes::INVALID_GRANT, "{} cannot be granted on the global with parameter level", access_flags.toString());

    throw Exception(ErrorCodes::INVALID_GRANT, "{} cannot be granted", access_flags.toString());
}

void AccessRightsElement::eraseNotGrantable()
{
    access_flags = getGrantableFlags();
}

void AccessRightsElement::replaceEmptyDatabase(const String & current_database)
{
    if (isEmptyDatabase())
        database = current_database;
}

void AccessRightsElement::replaceDeprecated()
{
    if (!access_flags)
        return;

    if (access_flags.toAccessTypes().size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "replaceDeprecated() was called on an access element with multiple access flags: {}", access_flags.toString());

    switch (const auto current_access_type = access_flags.toAccessTypes()[0])
    {
        case AccessType::FILE:
        case AccessType::URL:
        case AccessType::REMOTE:
        case AccessType::MONGO:
        case AccessType::REDIS:
        case AccessType::MYSQL:
        case AccessType::POSTGRES:
        case AccessType::SQLITE:
        case AccessType::ODBC:
        case AccessType::JDBC:
        case AccessType::HDFS:
        case AccessType::S3:
        case AccessType::HIVE:
        case AccessType::AZURE:
        case AccessType::KAFKA:
        case AccessType::NATS:
        case AccessType::RABBITMQ:
            if (!anyDatabase())
                /// This will leave statements like `REVOKE S3 ON system.*` untouched
                /// These statements will be deleted afterwards with `eraseNotGrantable()`
                break;
            access_flags = AccessType::READ | AccessType::WRITE;
            parameter = DB::toString(current_access_type);
            break;
        case AccessType::SOURCES:
            access_flags = AccessType::READ | AccessType::WRITE;
            break;
        default:
            break;
    }
}

void AccessRightsElement::makeBackwardCompatible()
{
    static const std::unordered_map<std::string, AccessType> string_to_accessType = {
        {"FILE", AccessType::FILE},
        {"URL", AccessType::URL},
        {"REMOTE", AccessType::REMOTE},
        {"MONGO", AccessType::MONGO},
        {"REDIS", AccessType::REDIS},
        {"MYSQL", AccessType::MYSQL},
        {"POSTGRES", AccessType::POSTGRES},
        {"SQLITE", AccessType::SQLITE},
        {"ODBC", AccessType::ODBC},
        {"JDBC", AccessType::JDBC},
        {"HDFS", AccessType::HDFS},
        {"S3", AccessType::S3},
        {"HIVE", AccessType::HIVE},
        {"AZURE", AccessType::AZURE},
        {"KAFKA", AccessType::KAFKA},
        {"NATS", AccessType::NATS},
        {"RABBITMQ", AccessType::RABBITMQ},
    };

    auto is_enabled_read_write_grants = false;
    if (const auto context = Context::getGlobalContextInstance())
    {
        const auto & access_control = context->getAccessControl();
        is_enabled_read_write_grants = access_control.isEnabledReadWriteGrants();
    }

    if (!is_enabled_read_write_grants)
    {
        if (access_flags == AccessType::READ || access_flags == AccessType::WRITE || access_flags == (AccessType::READ | AccessType::WRITE))
        {
            if (anyParameter())
            {
                access_flags = AccessType::SOURCES;
            }
            else
            {
                auto it = string_to_accessType.find(parameter);
                if (it != string_to_accessType.end())
                {
                    access_flags = it->second;
                    parameter.clear();
                }
            }
        }
    }
}

String AccessRightsElement::toString() const { return toStringImpl(*this, true); }
String AccessRightsElement::toStringWithoutOptions() const { return toStringImpl(*this, false); }

bool AccessRightsElements::empty() const { return std::all_of(begin(), end(), [](const AccessRightsElement & e) { return e.empty(); }); }

bool AccessRightsElements::sameDatabaseAndTableAndParameter() const
{
    return (size() < 2) || std::all_of(std::next(begin()), end(), [this](const AccessRightsElement & e) { return e.sameDatabaseAndTableAndParameter(front()); });
}

bool AccessRightsElements::sameDatabaseAndTable() const
{
    return (size() < 2) || std::all_of(std::next(begin()), end(), [this](const AccessRightsElement & e) { return e.sameDatabaseAndTable(front()); });
}

bool AccessRightsElements::sameOptions() const
{
    return (size() < 2) || std::all_of(std::next(begin()), end(), [this](const AccessRightsElement & e) { return e.sameOptions(front()); });
}

void AccessRightsElements::throwIfNotGrantable() const
{
    for (const auto & element : *this)
        element.throwIfNotGrantable();
}

void AccessRightsElements::eraseNotGrantable()
{
    std::erase_if(*this, [](AccessRightsElement & element)
    {
        element.eraseNotGrantable();
        return element.empty();
    });
}

void AccessRightsElements::replaceDeprecated()
{
    for (auto & element : *this)
        element.replaceDeprecated();
}

void AccessRightsElements::replaceEmptyDatabase(const String & current_database)
{
    for (auto & element : *this)
        element.replaceEmptyDatabase(current_database);
}

String AccessRightsElements::toString() const { return toStringImpl(*this, true); }
String AccessRightsElements::toStringWithoutOptions() const { return toStringImpl(*this, false); }

void AccessRightsElements::formatElementsWithoutOptions(WriteBuffer & buffer) const
{
    bool no_output = true;
    for (size_t i = 0; i != size(); ++i)
    {
        auto element = (*this)[i];
        element.makeBackwardCompatible();

        auto keywords = element.access_flags.toKeywords();
        if (keywords.empty() || (!element.anyColumn() && element.columns.empty()))
            continue;

        for (const auto & keyword : keywords)
        {
            if (!std::exchange(no_output, false))
                buffer << ", ";

            buffer << keyword;
            if (!element.anyColumn())
                element.formatColumnNames(buffer);
        }

        bool next_element_on_same_db_and_table = false;
        if (i != size() - 1)
        {
            const auto & next_element = (*this)[i + 1];
            if (element.sameDatabaseAndTableAndParameter(next_element))
            {
                next_element_on_same_db_and_table = true;
            }
        }

        if (!next_element_on_same_db_and_table)
        {
            buffer << " ";
            element.formatONClause(buffer);
        }
    }

    if (no_output)
        buffer << "USAGE ON " << "*.*";
}

}
