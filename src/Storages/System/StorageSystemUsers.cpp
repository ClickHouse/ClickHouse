#include <Storages/System/StorageSystemUsers.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Access/AccessControl.h>
#include <Access/Common/AccessFlags.h>
#include <Access/User.h>
#include <Backups/BackupEntriesCollector.h>
#include <Backups/RestorerFromBackup.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/Set.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/JSONString.h>

#include <base/types.h>
#include <base/range.h>

#include <sstream>


namespace DB
{
namespace
{
    DataTypeEnum8::Values getAuthenticationTypeEnumValues()
    {
        DataTypeEnum8::Values enum_values;
        for (auto type : collections::range(AuthenticationType::MAX))
            enum_values.emplace_back(AuthenticationTypeInfo::get(type).name, static_cast<Int8>(type));
        return enum_values;
    }

    /// The fast path performs `number of requested names * number of access storages` lookups.
    /// For a single user or a short `IN` list (the common case this optimization targets) this is
    /// far cheaper than scanning every user. But a very large requested set — for example
    /// `name IN (...)` with a huge literal list or subquery result — could perform more lookups
    /// than a single full scan, making the "fast" path slower than the fallback. Cap it at a
    /// deliberate limit so it can never become slower than the original behavior; above the limit
    /// we fall back to the full scan (exactly what happened before this optimization).
    constexpr size_t max_names_for_fast_path = 1000;

    bool isNameNode(const ActionsDAG::Node * node)
    {
        while (node->type == ActionsDAG::ActionType::ALIAS)
            node = node->children.at(0);
        /// Require the stripped node to be the actual `name` input column from
        /// `getFilterSampleBlock`, not merely something named `name`. Otherwise a constant or
        /// projection alias named `name` (for example `SELECT 'alice' AS name FROM system.users
        /// WHERE name = 'alice'`, where `name` in `WHERE` refers to the alias) would be mistaken
        /// for the column and diverge from the full-scan semantics.
        return node->type == ActionsDAG::ActionType::INPUT && node->result_name == "name";
    }

    /// Extract candidate user names from the predicate for O(1) lookups.
    /// Returns `nullopt` when no constraint on `name` can be derived (fall back to full scan).
    /// A returned set is a superset of names that can satisfy the predicate; the upstream
    /// filter still applies the full predicate to the produced rows. An empty set means the
    /// predicate is unsatisfiable on `name`, so the fast path emits nothing.
    /// Handles `name = 'literal'`, `name IN ('a', 'b')`, and `AND` conjunctions.
    std::optional<std::unordered_set<String>> extractNamesFromPredicateImpl(const ActionsDAG::Node & node, ContextPtr context)
    {
        if (node.type != ActionsDAG::ActionType::FUNCTION)
            return {};

        auto function_name = node.function_base->getName();

        if (function_name == "and")
        {
            /// `AND` requires intersecting children's candidate sets. Children that do not
            /// constrain `name` (return `nullopt`) contribute the universal set and are skipped;
            /// the remaining predicate is enforced by the upstream filter.
            std::optional<std::unordered_set<String>> result;
            for (const auto * child : node.children)
            {
                auto child_names = extractNamesFromPredicateImpl(*child, context);
                if (!child_names)
                    continue;
                if (!result)
                {
                    result = std::move(*child_names);
                    continue;
                }
                std::unordered_set<String> intersection;
                for (const auto & name : *result)
                {
                    if (child_names->contains(name))
                        intersection.insert(name);
                }
                result = std::move(intersection);
            }
            return result;
        }

        if (node.children.size() != 2)
            return {};

        if (function_name == "equals")
        {
            const ActionsDAG::Node * value = nullptr;

            if (isNameNode(node.children.at(0)))
                value = node.children.at(1);
            else if (isNameNode(node.children.at(1)))
                value = node.children.at(0);

            if (!value || !value->column)
                return {};

            if (!isString(removeNullable(removeLowCardinality(value->result_type))))
                return {};

            /// A `Nullable(String)` constant passes the type check above, but a `NULL` value
            /// (e.g. `name = CAST(NULL, 'Nullable(String)')`) matches no rows under full-scan
            /// semantics, and `getDataAt` would throw on it. Treat it as an unsatisfiable
            /// predicate on `name` by returning an empty candidate set, so the fast path emits
            /// nothing — consistent with the full scan. (`ColumnConst::isNullAt` forwards to the
            /// underlying value, so this is correct even though the constant has size 0.)
            if (value->column->isNullAt(0))
                return std::unordered_set<String>{};

            /// `ActionsDAG::addColumn` normalizes a constant node to a `ColumnConst` of size 0
            /// (see `ActionsDAG::Node::column`), so a size guard here would reject every literal
            /// and silently send `name = 'literal'` back to the full scan. The underlying data
            /// column still holds the value, so read it directly, like `StorageSystemZooKeeper`.
            std::unordered_set<String> names;
            names.insert(String(value->column->getDataAt(0)));
            return names;
        }

        if (function_name == "in")
        {
            if (!isNameNode(node.children.at(0)))
                return {};

            auto value = node.children.at(1)->column;
            if (!value)
                return {};

            const IColumn * column = value.get();
            if (const auto * column_const = typeid_cast<const ColumnConst *>(column))
                column = &column_const->getDataColumn();

            const auto * column_set = typeid_cast<const ColumnSet *>(column);
            if (!column_set)
                return {};

            auto future_set = column_set->getData();
            if (!future_set)
                return {};

            /// Check the set size *before* materializing its explicit elements. For an already
            /// built set (the literal `name IN ('a', 'b', ...)` case, which `FutureSetFromTuple`
            /// fills at construction time) `getTotalRowCount` is available without copying anything.
            /// Above the fast-path limit the full scan is cheaper, so fall back here instead of
            /// letting `buildOrderedSetInplace` duplicate the whole right-hand side first — its own
            /// guard (`use_index_for_in_with_subqueries_max_values`) defaults to `0` (unlimited)
            /// and would not stop a large literal list from being materialized.
            if (auto built_set = future_set->get(); built_set && built_set->getTotalRowCount() > max_names_for_fast_path)
                return {};

            /// `getTotalRowCount` above counts only *distinct* values. Building the explicit elements
            /// (`buildOrderedSetInplace` below → `Set::appendSetElements`) filters the *original*
            /// right-hand side columns, so its cost is proportional to the original list length, not
            /// the distinct count. A duplicate-heavy or mostly-`NULL` literal such as
            /// `name IN ('x', 'x', ..., 'y')` stays under the limit by distinct count yet would still
            /// scan a huge right-hand side here. For an explicit tuple set the original length is known
            /// in O(1), so bound on it too and fall back to the full scan above the limit.
            if (const auto * tuple_set = typeid_cast<const FutureSetFromTuple *>(future_set.get());
                tuple_set && tuple_set->getInputRowCount() > max_names_for_fast_path)
                return {};

            auto set = future_set->buildOrderedSetInplace(context);
            if (!set || !set->hasExplicitSetElements())
                return {};

            set->checkColumnsNumber(1);
            auto type = set->getElementsTypes()[0];
            if (!isString(removeNullable(removeLowCardinality(type))))
                return {};

            auto elements = set->getSetElements()[0];

            /// Final guard for set kinds whose size is known only after building (e.g. a set from a
            /// subquery, which `buildOrderedSetInplace` fills above): above the fast-path limit the
            /// full scan is cheaper, so fall back (returning `nullopt` leaves `name` unconstrained,
            /// which `fillData` turns into a full scan and `and` treats as the universal set). The
            /// upstream filter still enforces the `IN`.
            if (elements->size() > max_names_for_fast_path)
                return {};

            std::unordered_set<String> names;
            names.reserve(elements->size());
            for (size_t i = 0; i < elements->size(); ++i)
            {
                /// Skip `NULL` elements (e.g. `name IN (NULL, 'alice')`): they match no row under
                /// full-scan semantics, and `getDataAt` would throw on a `NULL`.
                if (elements->isNullAt(i))
                    continue;
                names.insert(String(elements->getDataAt(i)));
            }
            return names;
        }

        return {};
    }

    std::optional<std::unordered_set<String>> extractNamesFromPredicate(const ActionsDAG::Node * predicate, ContextPtr context)
    {
        if (!predicate)
            return {};
        return extractNamesFromPredicateImpl(*predicate, context);
    }
}


ColumnsDescription StorageSystemUsers::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "User name."},
        {"id", std::make_shared<DataTypeUUID>(), "User ID."},
        {"storage", std::make_shared<DataTypeString>(), "Path to the storage of users. Configured in the access_control_path parameter."},
        {"auth_type", std::make_shared<DataTypeArray>(std::make_shared<DataTypeEnum8>(getAuthenticationTypeEnumValues())),
            "Shows the authentication types. "
            "There are multiple ways of user identification: "
            "with no password, with plain text password, with SHA256-encoded password, "
            "with double SHA-1-encoded password or with bcrypt-encoded password."
        },
        {"auth_params", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "Authentication parameters in the JSON format depending on the auth_type."
        },
        {"valid_until", std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>()),
            "The expiration date and time for user credentials."
        },
        {"host_ip", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "IP addresses of hosts that are allowed to connect to the ClickHouse server."
        },
        {"host_names", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "Names of hosts that are allowed to connect to the ClickHouse server."
        },
        {"host_names_regexp", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "Regular expression for host names that are allowed to connect to the ClickHouse server."
        },
        {"host_names_like", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "Names of hosts that are allowed to connect to the ClickHouse server, set using the LIKE predicate."
        },
        {"default_roles_all", std::make_shared<DataTypeUInt8>(),
            "Shows that all granted roles set for user by default."
        },
        {"default_roles_list", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "List of granted roles provided by default."
        },
        {"default_roles_except", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "All the granted roles set as default excepting of the listed ones."
        },
        {"grantees_any", std::make_shared<DataTypeUInt8>(), "The flag that indicates whether a user with any grant option can grant it to anyone."},
        {"grantees_list", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "The list of users or roles to which this user is allowed to grant options to."},
        {"grantees_except", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "The list of users or roles to which this user is forbidden from grant options to."},
        {"default_database", std::make_shared<DataTypeString>(), "The name of the default database for this user."},
    };
}


void StorageSystemUsers::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node * predicate, std::vector<UInt8>) const
{
    /// If "select_from_system_db_requires_grant" is enabled the access rights were already checked in InterpreterSelectQuery.
    const auto & access_control = context->getAccessControl();
    if (!access_control.doesSelectFromSystemDatabaseRequireGrant())
        context->checkAccess(AccessType::SHOW_USERS);

    size_t column_index = 0;
    auto & column_name = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_id = assert_cast<ColumnUUID &>(*res_columns[column_index++]).getData();
    auto & column_storage = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_auth_type = assert_cast<ColumnInt8 &>(assert_cast<ColumnArray &>(*res_columns[column_index]).getData());
    auto & column_auth_type_offsets =  assert_cast<ColumnArray &>(*res_columns[column_index++]).getOffsets();
    auto & column_auth_params = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[column_index]).getData());
    auto & column_auth_params_offsets = assert_cast<ColumnArray &>(*res_columns[column_index++]).getOffsets();
    auto & column_valid_until = assert_cast<ColumnUInt32 &>(assert_cast<ColumnArray &>(*res_columns[column_index]).getData());
    auto & column_valid_until_offsets = assert_cast<ColumnArray &>(*res_columns[column_index++]).getOffsets();
    auto & column_host_ip = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[column_index]).getData());
    auto & column_host_ip_offsets = assert_cast<ColumnArray &>(*res_columns[column_index++]).getOffsets();
    auto & column_host_names = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[column_index]).getData());
    auto & column_host_names_offsets = assert_cast<ColumnArray &>(*res_columns[column_index++]).getOffsets();
    auto & column_host_names_regexp = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[column_index]).getData());
    auto & column_host_names_regexp_offsets = assert_cast<ColumnArray &>(*res_columns[column_index++]).getOffsets();
    auto & column_host_names_like = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[column_index]).getData());
    auto & column_host_names_like_offsets = assert_cast<ColumnArray &>(*res_columns[column_index++]).getOffsets();
    auto & column_default_roles_all = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();
    auto & column_default_roles_list = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[column_index]).getData());
    auto & column_default_roles_list_offsets = assert_cast<ColumnArray &>(*res_columns[column_index++]).getOffsets();
    auto & column_default_roles_except = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[column_index]).getData());
    auto & column_default_roles_except_offsets = assert_cast<ColumnArray &>(*res_columns[column_index++]).getOffsets();
    auto & column_grantees_any = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();
    auto & column_grantees_list = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[column_index]).getData());
    auto & column_grantees_list_offsets = assert_cast<ColumnArray &>(*res_columns[column_index++]).getOffsets();
    auto & column_grantees_except = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[column_index]).getData());
    auto & column_grantees_except_offsets = assert_cast<ColumnArray &>(*res_columns[column_index++]).getOffsets();
    auto & column_default_database = assert_cast<ColumnString &>(*res_columns[column_index++]);

    auto add_row = [&](const String & name,
                       const UUID & id,
                       const String & storage_name,
                       const std::vector<AuthenticationData> & authentication_methods,
                       const AllowedClientHosts & allowed_hosts,
                       const RolesOrUsersSet & default_roles,
                       const RolesOrUsersSet & grantees,
                       const String default_database)
    {
        column_name.insertData(name.data(), name.length());
        column_id.push_back(id.toUnderType());
        column_storage.insertData(storage_name.data(), storage_name.length());

        for (const auto & auth_data : authentication_methods)
        {
            Poco::JSON::Object auth_params_json;

            if (auth_data.getType() == AuthenticationType::LDAP)
            {
                auth_params_json.set("server", auth_data.getLDAPServerName());
            }
            else if (auth_data.getType() == AuthenticationType::KERBEROS)
            {
                auth_params_json.set("realm", auth_data.getKerberosRealm());
            }
#if USE_SSL
            else if (auth_data.getType() == AuthenticationType::SSL_CERTIFICATE)
            {
                Poco::JSON::Array::Ptr common_names = new Poco::JSON::Array();
                Poco::JSON::Array::Ptr subject_alt_names = new Poco::JSON::Array();

                const auto & subjects = auth_data.getSSLCertificateSubjects();
                for (const String & subject : subjects.at(X509Certificate::Subjects::Type::CN))
                    common_names->add(subject);
                for (const String & subject : subjects.at(X509Certificate::Subjects::Type::SAN))
                    subject_alt_names->add(subject);

                if (common_names->size() > 0)
                    auth_params_json.set("common_names", common_names);
                if (subject_alt_names->size() > 0)
                    auth_params_json.set("subject_alt_names", subject_alt_names);
            }
#endif
            else if (const auto & otp_data = auth_data.getOneTimePassword(); otp_data.has_value())
            {
                auth_params_json.set("second_factor", "one_time_password");
                if (otp_data->params != OneTimePasswordParams{})
                {
                    auth_params_json.set("otp_algorithm", toString(otp_data->params.algorithm));
                    auth_params_json.set("otp_num_digits", toString(otp_data->params.num_digits));
                    auth_params_json.set("otp_period", toString(otp_data->params.period));
                }
            }

            std::ostringstream oss;         // STYLE_CHECK_ALLOW_STD_STRING_STREAM
            oss.exceptions(std::ios::failbit);
            Poco::JSON::Stringifier::stringify(auth_params_json, oss);
            const auto authentication_params_str = oss.str();

            column_auth_params.insertData(authentication_params_str.data(), authentication_params_str.size());
            column_auth_type.insertValue(static_cast<Int8>(auth_data.getType()));
            column_valid_until.insertValue(static_cast<UInt32>(auth_data.getValidUntil()));
        }

        column_auth_params_offsets.push_back(column_auth_params.size());
        column_auth_type_offsets.push_back(column_auth_type.size());
        column_valid_until_offsets.push_back(column_valid_until.size());

        if (allowed_hosts.containsAnyHost())
        {
            static constexpr std::string_view str{"::/0"};
            column_host_ip.insertData(str.data(), str.length());
        }
        else
        {
            if (allowed_hosts.containsLocalHost())
            {
                static constexpr std::string_view str{"localhost"};
                column_host_names.insertData(str.data(), str.length());
            }

            for (const auto & ip : allowed_hosts.getAddresses())
            {
                String str = ip.toString();
                column_host_ip.insertData(str.data(), str.length());
            }
            for (const auto & subnet : allowed_hosts.getSubnets())
            {
                String str = subnet.toString();
                column_host_ip.insertData(str.data(), str.length());
            }

            for (const auto & host_name : allowed_hosts.getNames())
                column_host_names.insertData(host_name.data(), host_name.length());

            for (const auto & name_regexp : allowed_hosts.getNameRegexps())
                column_host_names_regexp.insertData(name_regexp.data(), name_regexp.length());

            for (const auto & like_pattern : allowed_hosts.getLikePatterns())
                column_host_names_like.insertData(like_pattern.data(), like_pattern.length());
        }

        column_host_ip_offsets.push_back(column_host_ip.size());
        column_host_names_offsets.push_back(column_host_names.size());
        column_host_names_regexp_offsets.push_back(column_host_names_regexp.size());
        column_host_names_like_offsets.push_back(column_host_names_like.size());

        auto default_roles_ast = default_roles.toASTWithNames(access_control);
        column_default_roles_all.push_back(default_roles_ast->all);
        for (const auto & role_name : default_roles_ast->names)
            column_default_roles_list.insertData(role_name.data(), role_name.length());
        column_default_roles_list_offsets.push_back(column_default_roles_list.size());
        for (const auto & except_name : default_roles_ast->except_names)
            column_default_roles_except.insertData(except_name.data(), except_name.length());
        column_default_roles_except_offsets.push_back(column_default_roles_except.size());

        auto grantees_ast = grantees.toASTWithNames(access_control);
        column_grantees_any.push_back(grantees_ast->all);
        for (const auto & grantee_name : grantees_ast->names)
            column_grantees_list.insertData(grantee_name.data(), grantee_name.length());
        column_grantees_list_offsets.push_back(column_grantees_list.size());
        for (const auto & except_name : grantees_ast->except_names)
            column_grantees_except.insertData(except_name.data(), except_name.length());
        column_grantees_except_offsets.push_back(column_grantees_except.size());

        column_default_database.insertData(default_database.data(), default_database.length());
    };

    /// Fast path: if predicate contains `name = 'literal'` or `name IN (...)`,
    /// do O(1) lookups instead of iterating all users. The extraction already caps the candidate
    /// set at `max_names_for_fast_path`; the check below is a defensive guard kept in sync with it.
    auto names = extractNamesFromPredicate(predicate, context);
    if (names && names->size() <= max_names_for_fast_path)
    {
        /// A name can exist in more than one access storage (for example a user defined in a
        /// local directory shadowing one from `users.xml`), and the full scan over `findAll<User>`
        /// emits a row for each. Look the name up in every storage so the fast path keeps the same
        /// semantics instead of silently dropping the lower-precedence rows. This stays
        /// O(number of requested names * number of access storages).
        ///
        /// Resolve the matched id through `AccessControl` (the same `tryRead` and `findStorage`
        /// the fallback uses) rather than reading from `storage` directly. Two `users_xml`
        /// storages defining the same name produce the same generated `UUID`, which always
        /// resolves to the first storage; reading from the specific `storage` instead would
        /// attribute the row to the wrong storage and diverge from the full scan.
        auto storages = access_control.getStorages();
        for (const auto & name : *names)
        {
            for (const auto & storage : storages)
            {
                auto id = storage->find<User>(name);
                if (!id)
                    continue;

                auto user = access_control.tryRead<User>(*id);
                if (!user)
                    continue;

                auto id_storage = access_control.findStorage(*id);
                if (!id_storage)
                    continue;

                add_row(user->getName(), *id, id_storage->getStorageName(), user->authentication_methods, user->allowed_client_hosts,
                        user->default_roles, user->grantees, user->default_database);
            }
        }
        return;
    }

    /// Fallback: iterate all users when names cannot be extracted from the predicate.
    std::vector<UUID> ids = access_control.findAll<User>();
    for (const auto & id : ids)
    {
        auto user = access_control.tryRead<User>(id);
        if (!user)
            continue;

        auto storage = access_control.findStorage(id);
        if (!storage)
            continue;

        add_row(user->getName(), id, storage->getStorageName(), user->authentication_methods, user->allowed_client_hosts,
                user->default_roles, user->grantees, user->default_database);
    }
}

Block StorageSystemUsers::getFilterSampleBlock() const
{
    return {
        { {}, std::make_shared<DataTypeString>(), "name" },
    };
}

void StorageSystemUsers::backupData(
    BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & /* partitions */)
{
    const auto & access_control = backup_entries_collector.getContext()->getAccessControl();
    access_control.backup(backup_entries_collector, data_path_in_backup, AccessEntityType::USER);
}

void StorageSystemUsers::restoreDataFromBackup(
    RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & /* partitions */)
{
    auto & access_control = restorer.getContext()->getAccessControl();
    access_control.restoreFromBackup(restorer, data_path_in_backup);
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemUsers) }
