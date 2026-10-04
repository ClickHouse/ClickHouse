#pragma once

#include <Core/ColumnsWithTypeAndName.h>

#include <arrow/result.h>
#include <arrow/type.h>

#include <cstdint>
#include <span>
#include <string_view>


namespace DB::ArrowFlight
{

inline constexpr int32_t XDBC_SQL_DATETIME = 9;
inline constexpr int32_t XDBC_SQL_NULLABLE = 1;

/// One row of `CommandGetXdbcTypeInfo`. Rows are ordered by `(data_type, type_name)`.
struct XdbcTypeInfoRow
{
    std::string_view type_name;
    int32_t data_type = 0;
    int32_t column_size = 0;
    const char * literal_prefix = nullptr;
    const char * literal_suffix = nullptr;
    /// Comma-separated parameter keywords (e.g. "precision,scale"); empty means NULL.
    std::string_view create_params = {};
    bool case_sensitive = false;
    int32_t searchable = 0;
    bool numeric = false;
    bool unsigned_attribute = false;
    bool fixed_prec_scale = false;
    /// -1 means NULL; when set, `sql_data_type` is `SQL_DATETIME` and this is the subcode.
    int32_t datetime_subcode = -1;
    int32_t minimum_scale = -1; /// -1 means NULL
    int32_t maximum_scale = -1;
    int32_t num_prec_radix = -1;
};

std::span<const XdbcTypeInfoRow> getXdbcTypeInfoRows();

const XdbcTypeInfoRow * findXdbcTypeInfo(std::string_view type_name);

arrow::Result<std::shared_ptr<arrow::Schema>>
addFlightSQLTypeMetadata(std::shared_ptr<arrow::Schema> schema, const ColumnsWithTypeAndName & header);

}
