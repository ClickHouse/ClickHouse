#pragma once

#include <string_view>


namespace DB
{

/** The full reference-page template embedded for a system table.
  *
  * Templates use `{{SYSTEM_TABLE_COLUMNS}}` for the column list assembled from
  * the live `ColumnsDescription`. A few introspection tables also use a
  * placeholder for the catalog they expose (`{{PROFILE_EVENTS}}`,
  * `{{CURRENT_METRICS}}`, or `{{ASYNCHRONOUS_METRICS}}`).
  */
struct SystemTableDocumentation
{
    std::string_view page_template;
    std::string_view source;
};

/// Returns the embedded full-page documentation for `table_name`, or `nullptr`
/// for a table that has not been migrated yet.
const SystemTableDocumentation * getSystemTableDocumentation(std::string_view table_name);

}
