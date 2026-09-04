#include <Interpreters/SessionColumnsCommon.h>

#include <Access/Common/AuthenticationType.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <Interpreters/ClientInfo.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <base/EnumReflection.h>

namespace DB
{

namespace
{
    using AuthType = AuthenticationType;
    using Interface = ClientInfo::Interface;
}

DataTypeEnum8::Values getSessionAuthTypeEnumValues()
{
#define AUTH_TYPE_NAME_AND_VALUE(v) std::make_pair(toString(v), static_cast<Int8>(v))
    DataTypeEnum8::Values values
    {
        AUTH_TYPE_NAME_AND_VALUE(AuthType::NO_PASSWORD),
        AUTH_TYPE_NAME_AND_VALUE(AuthType::PLAINTEXT_PASSWORD),
        AUTH_TYPE_NAME_AND_VALUE(AuthType::SHA256_PASSWORD),
        AUTH_TYPE_NAME_AND_VALUE(AuthType::DOUBLE_SHA1_PASSWORD),
        AUTH_TYPE_NAME_AND_VALUE(AuthType::LDAP),
        AUTH_TYPE_NAME_AND_VALUE(AuthType::JWT),
        AUTH_TYPE_NAME_AND_VALUE(AuthType::KERBEROS),
        AUTH_TYPE_NAME_AND_VALUE(AuthType::SSH_KEY),
        AUTH_TYPE_NAME_AND_VALUE(AuthType::SSL_CERTIFICATE),
        AUTH_TYPE_NAME_AND_VALUE(AuthType::BCRYPT_PASSWORD),
        AUTH_TYPE_NAME_AND_VALUE(AuthType::HTTP),
        AUTH_TYPE_NAME_AND_VALUE(AuthType::SCRAM_SHA256_PASSWORD),
        AUTH_TYPE_NAME_AND_VALUE(AuthType::NO_AUTHENTICATION),
    };
#undef AUTH_TYPE_NAME_AND_VALUE
    static_assert(static_cast<int>(AuthenticationType::MAX) == 13);
    return values;
}

DataTypeEnum8::Values getSessionInterfaceEnumValues()
{
    DataTypeEnum8::Values values
    {
        {"TCP",             static_cast<Int8>(Interface::TCP)},
        {"HTTP",            static_cast<Int8>(Interface::HTTP)},
        {"gRPC",            static_cast<Int8>(Interface::GRPC)},
        {"MySQL",           static_cast<Int8>(Interface::MYSQL)},
        {"PostgreSQL",      static_cast<Int8>(Interface::POSTGRESQL)},
        {"Local",           static_cast<Int8>(Interface::LOCAL)},
        {"TCP_Interserver", static_cast<Int8>(Interface::TCP_INTERSERVER)},
        {"Prometheus",      static_cast<Int8>(Interface::PROMETHEUS)},
        {"Background",      static_cast<Int8>(Interface::BACKGROUND)},
        {"ArrowFlight",     static_cast<Int8>(Interface::ARROW_FLIGHT)},
    };
    static_assert(magic_enum::enum_count<Interface>() == 10, "Please update the array above to match the enum.");
    return values;
}

DataTypePtr getNameValueArrayType(const DataTypePtr & name_type, const DataTypePtr & value_type)
{
    return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes({name_type, value_type})));
}

void fillStringArrayColumn(const Strings & data, IColumn & column)
{
    auto & array = typeid_cast<ColumnArray &>(column);
    auto & data_col = array.getData();
    for (const auto & name : data)
        data_col.insertData(name.data(), name.size());
    auto & offsets = array.getOffsets();
    offsets.push_back(offsets.back() + data.size());
}

void fillNameValueArrayColumn(const std::vector<std::pair<String, String>> & data, IColumn & column)
{
    auto & array_col = assert_cast<ColumnArray &>(column);
    auto & tuple_col = assert_cast<ColumnTuple &>(array_col.getData());
    auto & names_col = *tuple_col.getColumnPtr(0)->assumeMutable();
    auto & values_col = *tuple_col.getColumnPtr(1)->assumeMutable();

    for (const auto & kv : data)
    {
        names_col.insert(kv.first);
        values_col.insert(kv.second);
    }

    auto & offsets = array_col.getOffsets();
    offsets.push_back(tuple_col.size());
}

void fillCertificateColumns(const std::optional<ClientCertificateInfo> & certificate_info, MutableColumns & columns, size_t & i)
{
    if (certificate_info)
    {
        fillStringArrayColumn(certificate_info->subjects, *columns[i++]);
        columns[i++]->insert(certificate_info->serial);
        columns[i++]->insert(certificate_info->issuer);
        columns[i++]->insert(DecimalField<DateTime64>(certificate_info->not_before, 0));
        columns[i++]->insert(DecimalField<DateTime64>(certificate_info->not_after, 0));
    }
    else
    {
        for (size_t k = 0; k < 5; ++k)
            columns[i++]->insertDefault();
    }
}

}
