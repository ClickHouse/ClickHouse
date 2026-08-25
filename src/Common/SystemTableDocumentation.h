#pragma once

#include <base/types.h>

#include <source_location>
#include <string_view>


namespace DB
{

/** Structured documentation used to generate a system-table reference page.
  * Column definitions are intentionally absent: they are rendered from the
  * live `ColumnsDescription` of the table. `columns_notes` can add narrative
  * or caveats after that generated list.
  */
struct SystemTableDocumentation
{
    String description;
    String columns_notes;
    String examples;
    String additional_sections;
    String see_also;

    /// Captured at the registration site so `system.documentation` points to
    /// the C++ file which owns this documentation.
    const char * source = std::source_location::current().file_name();
};

void registerSystemTableDocumentation(std::string_view table_name, SystemTableDocumentation documentation);

/// Returns the documentation registered for `table_name`, or `nullptr` when
/// an optional/private table has not provided structured documentation.
const SystemTableDocumentation * getSystemTableDocumentation(std::string_view table_name);

}

#define REGISTER_SYSTEM_TABLE_DOCUMENTATION_CONCAT_IMPL(a, b) a##b
#define REGISTER_SYSTEM_TABLE_DOCUMENTATION_CONCAT(a, b) REGISTER_SYSTEM_TABLE_DOCUMENTATION_CONCAT_IMPL(a, b)

#if defined(__clang__)
#    define SYSTEM_TABLE_DOCUMENTATION_DIAGNOSTIC_PUSH \
        _Pragma("clang diagnostic push") \
        _Pragma("clang diagnostic ignored \"-Wmissing-designated-field-initializers\"")
#    define SYSTEM_TABLE_DOCUMENTATION_DIAGNOSTIC_POP _Pragma("clang diagnostic pop")
#else
#    define SYSTEM_TABLE_DOCUMENTATION_DIAGNOSTIC_PUSH
#    define SYSTEM_TABLE_DOCUMENTATION_DIAGNOSTIC_POP
#endif

/// Place at file scope in the C++ file which defines the system table's schema.
#define REGISTER_SYSTEM_TABLE_DOCUMENTATION(TABLE_NAME, ...) \
    SYSTEM_TABLE_DOCUMENTATION_DIAGNOSTIC_PUSH \
    namespace \
    { \
        [[maybe_unused]] const bool \
            REGISTER_SYSTEM_TABLE_DOCUMENTATION_CONCAT(registered_system_table_documentation_, __LINE__) \
            = (::DB::registerSystemTableDocumentation( \
                   TABLE_NAME, ::DB::SystemTableDocumentation{__VA_ARGS__}), \
               true); \
    } \
    SYSTEM_TABLE_DOCUMENTATION_DIAGNOSTIC_POP
