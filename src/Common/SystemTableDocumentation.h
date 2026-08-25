#pragma once

#include <base/types.h>

#include <source_location>
#include <string_view>
#include <unordered_map>


namespace DB
{

class ColumnsDescription;

using SystemTableColumnsProvider = ColumnsDescription (*)();

/** Structured documentation used to generate a system-table reference page.
  * Columns normally come from the live table metadata. Tables which are not
  * attached in every environment provide `get_columns` so their complete page
  * can still be rendered directly from this registry. `columns_notes` can add
  * narrative or caveats after the generated list.
  */
struct SystemTableDocumentation
{
    String description;
    SystemTableColumnsProvider get_columns = nullptr;
    String columns_notes;
    String examples;
    String additional_sections;
    String see_also;

    /// Captured at the registration site so `system.documentation` points to
    /// the C++ file which owns this documentation.
    const char * source = std::source_location::current().file_name();
};

using SystemTableDocumentationRegistry = std::unordered_map<String, SystemTableDocumentation>;

void registerSystemTableDocumentation(std::string_view table_name, SystemTableDocumentation documentation);

/// Returns every registered system-table document, including tables which are
/// not attached in the current server or `clickhouse-local` environment.
const SystemTableDocumentationRegistry & getSystemTableDocumentationRegistry();

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
        [[maybe_unused, gnu::used, gnu::retain]] const bool \
            REGISTER_SYSTEM_TABLE_DOCUMENTATION_CONCAT(registered_system_table_documentation_, __LINE__) \
            = (::DB::registerSystemTableDocumentation( \
                   TABLE_NAME, ::DB::SystemTableDocumentation{__VA_ARGS__}), \
               true); \
    } \
    SYSTEM_TABLE_DOCUMENTATION_DIAGNOSTIC_POP
