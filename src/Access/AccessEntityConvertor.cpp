#include <memory>
#include <Common/Exception.h>
#include <Access/User.h>
#include <Access/Role.h>
#include <Access/RowPolicy.h>
#include <Access/SettingsProfile.h>
#include <Access/Quota.h>
#include <Access/IAccessEntity.h>
#include <Access/Common/AccessEntityType.h>
#include <MetadataACLEntry.pb.h>
#include "AccessEntityConvertor.h"
#include <Access/AccessEntityIO.h>
#include <Common/FoundationDB/ProtobufTypeHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_PARSE_PROTOBUF_SCHEMA;
}

namespace FoundationDB
{
    namespace
    {
        inline void assertAccessEntityType(const IAccessEntity & entity, AccessEntityType type)
        {
            if (!entity.isTypeOf(type))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Entity type is {}, expect {}", toString(entity.getType()), toString(type));
        }

        template <class ProtoType>
        ProtoType accessEntityToProto(const IAccessEntity & entity);


        template <>
        Proto::MetadataUserEntry accessEntityToProto(const IAccessEntity & entity)
        {
            assertAccessEntityType(entity, AccessEntityType::USER);

            const auto & user = dynamic_cast<const User &>(entity);

            Proto::MetadataUserEntry proto;

            proto.set_default_database(user.default_database);
            proto.set_grantees(user.grantees.toString());
            proto.set_default_roles(user.default_roles.toString());

            /// SettingsProfileComponent
            for (auto const & setting : user.settings)
            {
                auto * tmp = proto.mutable_settings()->Add();
                auto setting_tuple = setting.toTuple();
                tmp->set_setting_name(std::get<1>(setting_tuple));
                tmp->set_value(toString(std::get<2>(setting_tuple)));
                tmp->set_max_value(toString(std::get<3>(setting_tuple)));
                tmp->set_min_value(toString(std::get<4>(setting_tuple)));
                tmp->set_readonly(static_cast<bool>(std::get<5>(setting_tuple)));
            }

            /// AccessRightsComponent
            for (const auto & element : user.access.getElements())
            {
                auto * tmp = proto.mutable_access()->Add();
                tmp->set_access_flags(element.access_flags.toString());
                tmp->set_database(element.database);
                tmp->set_table(element.table);

                for (const auto & col : element.columns)
                    tmp->add_columns(col);
            }

            /// GrantedRoleComponent
            for (const auto & element : user.granted_roles.getElements())
            {
                auto * tmp = proto.mutable_granted_roles()->Add();
                tmp->set_admin_option(element.admin_option);
                tmp->set_is_empty(element.empty());

                for (const auto & id : element.ids)
                {
                    auto * uuid = tmp->mutable_ids()->Add();
                    toProtoUUID(id, *uuid);
                }
            }

            /// allowed_client_hosts_IPAddress
            for (const auto & ip_address : user.allowed_client_hosts.getAddresses())
            {
                std::string * tmp = proto.add_allowed_client_hosts_ipaddress();
                *tmp = ip_address.toString();
            }

            /// allowed_client_hosts_IPSubnet
            for (const auto & ip_subnet : user.allowed_client_hosts.getSubnets())
            {
                std::string * tmp = proto.add_allowed_client_hosts_ipsubnet();
                *tmp = ip_subnet.toString();
            }

            /// allowed_client_hosts_names
            for (const auto & host_name : user.allowed_client_hosts.getNames())
            {
                std::string * tmp = proto.add_allowed_client_hosts_names();
                *tmp = host_name;
            }

            /// allowed_client_hosts_names_regexps
            for (const auto & names_regexps : user.allowed_client_hosts.getNameRegexps())
            {
                std::string * tmp = proto.add_allowed_client_hosts_names_regexps();
                *tmp = names_regexps;
            }

            /// allowed_client_hosts_names_like
            for (const auto & names_like : user.allowed_client_hosts.getLikePatterns())
            {
                std::string * tmp = proto.add_allowed_client_hosts_names_like();
                *tmp = names_like;
            }

            return proto;
        }


        template <>
        Proto::MetadataRoleEntry accessEntityToProto(const IAccessEntity & entity)
        {
            assertAccessEntityType(entity, AccessEntityType::ROLE);

            const auto & role = dynamic_cast<const Role &>(entity);
            Proto::MetadataRoleEntry proto;

            /// GrantedRoleComponent
            for (const auto & element : role.granted_roles.getElements())
            {
                auto * tmp = proto.mutable_granted_roles()->Add();
                tmp->set_admin_option(element.admin_option);
                tmp->set_is_empty(element.empty());

                for (const auto & id : element.ids)
                {
                    auto * uuid = tmp->mutable_ids()->Add();
                    toProtoUUID(id, *uuid);
                }
            }

            ///AccessRightsComponent
            for (const auto & element : role.access.getElements())
            {
                auto * tmp = proto.mutable_access()->Add();
                tmp->set_access_flags(element.access_flags.toString());
                tmp->set_database(element.database);
                tmp->set_table(element.table);

                for (const auto & col : element.columns)
                    tmp->add_columns(col);
            }

            /// SettingsProfileComponent
            for (auto const & setting : role.settings)
            {
                auto * tmp = proto.mutable_settings()->Add();
                auto setting_tuple = setting.toTuple();
                tmp->set_setting_name(std::get<1>(setting_tuple));
                tmp->set_value(toString(std::get<2>(setting_tuple)));
                tmp->set_max_value(toString(std::get<3>(setting_tuple)));
                tmp->set_min_value(toString(std::get<4>(setting_tuple)));
                tmp->set_readonly(static_cast<bool>(std::get<5>(setting_tuple)));
            }
            return proto;
        }


        template <>
        Proto::MetadataQuotaEntry accessEntityToProto(const IAccessEntity & entity)
        {
            assertAccessEntityType(entity, AccessEntityType::QUOTA);
            Proto::MetadataQuotaEntry proto;

            const auto & quota = dynamic_cast<const Quota &>(entity);

            proto.set_key_type(toString(quota.getType()));
            proto.set_to_roles(quota.to_roles.toString());

            for (const auto & limit : quota.all_limits)
            {
                auto * tmp = proto.mutable_limits()->Add();
                tmp->set_duration(limit.duration.count());
                tmp->set_randomize_interval(limit.randomize_interval);
            }
            return proto;
        }


        template <>
        Proto::MetadataRowPolicyEntry accessEntityToProto(const IAccessEntity & entity)
        {
            assertAccessEntityType(entity, AccessEntityType::ROW_POLICY);
            Proto::MetadataRowPolicyEntry proto;

            const auto & row_policy = dynamic_cast<const RowPolicy &>(entity);

            proto.set_is_permissive(row_policy.isPermissive());
            proto.set_is_restrictive(row_policy.isRestrictive());
            proto.set_to_roles(row_policy.to_roles.toString());
            proto.set_database(row_policy.getDatabase());
            proto.set_full_name(row_policy.getFullName().toString());
            proto.set_table_name(row_policy.getTableName());

            return proto;
        }


        template <>
        Proto::MetadataSettingsProfileEntry accessEntityToProto(const IAccessEntity & entity)
        {
            assertAccessEntityType(entity, AccessEntityType::SETTINGS_PROFILE);
            Proto::MetadataSettingsProfileEntry proto;
            const auto & settings_profile = dynamic_cast<const SettingsProfile &>(entity);

            proto.set_to_roles(settings_profile.to_roles.toString());

            /// SettingsProfileComponent
            for (auto const & setting : settings_profile.elements)
            {
                auto * tmp = proto.mutable_settings()->Add();
                auto setting_tuple = setting.toTuple();
                tmp->set_setting_name(std::get<1>(setting_tuple));
                tmp->set_value(toString(std::get<2>(setting_tuple)));
                tmp->set_max_value(toString(std::get<3>(setting_tuple)));
                tmp->set_min_value(toString(std::get<4>(setting_tuple)));
                tmp->set_readonly(static_cast<bool>(std::get<5>(setting_tuple)));
            }

            return proto;
        }
    }

    Proto::AccessEntity toProto(const IAccessEntity & entity)
    {
        Proto::AccessEntity proto;
        auto definition = DB::serializeAccessEntity(entity);
        proto.set_name(entity.getName());
        proto.set_definition(definition);

        switch (entity.getType())
        {
            case AccessEntityType::USER:
                *proto.mutable_user() = accessEntityToProto<Proto::MetadataUserEntry>(entity);
                break;
            case AccessEntityType::ROLE:
                *proto.mutable_role() = accessEntityToProto<Proto::MetadataRoleEntry>(entity);
                break;
            case AccessEntityType::QUOTA:
                *proto.mutable_quota() = accessEntityToProto<Proto::MetadataQuotaEntry>(entity);
                break;
            case AccessEntityType::ROW_POLICY:
                *proto.mutable_row_policy() = accessEntityToProto<Proto::MetadataRowPolicyEntry>(entity);
                break;
            case AccessEntityType::SETTINGS_PROFILE:
                *proto.mutable_settings_profile() = accessEntityToProto<Proto::MetadataSettingsProfileEntry>(entity);
                break;
            case AccessEntityType::MAX:
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported access entity type: {}", toString(entity.getType()));
        }

        return proto;
    }

    std::shared_ptr<const IAccessEntity> fromProto(const Proto::AccessEntity & proto)
    {
        return DB::deserializeAccessEntity(proto.definition());
    }

}
}
