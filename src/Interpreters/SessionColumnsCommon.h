#pragma once

#include <Columns/IColumn_fwd.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/ClientCertificateInfo.h>

#include <optional>
#include <utility>
#include <vector>

namespace DB
{

/// system.session_log and system.sessions share most of their columns (auth type, interface,
/// settings/quotas as name-value pairs, TLS certificate info). These helpers keep the column
/// definitions and fill logic in one place instead of duplicating them in both storages.

DataTypeEnum8::Values getSessionAuthTypeEnumValues();
DataTypeEnum8::Values getSessionInterfaceEnumValues();

/// An array of Tuple(name, value), e.g. for "settings" or "quotas" columns.
DataTypePtr getNameValueArrayType(const DataTypePtr & name_type, const DataTypePtr & value_type);

void fillStringArrayColumn(const Strings & data, IColumn & column);
void fillNameValueArrayColumn(const std::vector<std::pair<String, String>> & data, IColumn & column);

/// Fills the 5 certificate_* columns (subjects, serial, issuer, not_before, not_after) starting at columns[i],
/// advancing i past them.
void fillCertificateColumns(const std::optional<ClientCertificateInfo> & certificate_info, MutableColumns & columns, size_t & i);

}
