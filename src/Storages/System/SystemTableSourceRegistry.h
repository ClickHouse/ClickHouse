#pragma once

#include <typeinfo>

namespace DB
{

/** Registry mapping a system table storage class to the source file where it is implemented, so that
  * `system.documentation` can report each system table's own source file.
  *
  * There is no central place that knows each system table's source file: the tables are attached uniformly in
  * `attachSystemTables.cpp`, and `std::source_location` captured there (or in the base class constructor) resolves
  * to the wrong file. Instead, each system table registers its source from its own `.cpp` via
  * `REGISTER_SYSTEM_TABLE_SOURCE`, so that `__builtin_FILE()` resolves to that `.cpp`. Storages that are not
  * registered report an empty source.
  */
void registerSystemTableSource(const std::type_info & type, const char * source_file);

/// The source file registered for the given storage type, or `nullptr` if it was not registered. The returned
/// path is as produced by the compiler; the caller normalizes it to be relative to the repository root.
const char * getSystemTableSource(const std::type_info & type);

}

/// Place at file scope in a system table's `.cpp` (so that `__builtin_FILE()` resolves to that file). Accepts a type,
/// including a template instantiation, e.g. `REGISTER_SYSTEM_TABLE_SOURCE(::DB::SystemMergeTreeSettings<false>)`.
#define REGISTER_SYSTEM_TABLE_SOURCE_CONCAT_IMPL(a, b) a##b
#define REGISTER_SYSTEM_TABLE_SOURCE_CONCAT(a, b) REGISTER_SYSTEM_TABLE_SOURCE_CONCAT_IMPL(a, b)
/// `gnu::used` + `gnu::retain` keep the registration's static initializer from being discarded by the compiler or by
/// the linker's `--gc-sections`/`--icf` (the variable has internal linkage and is otherwise never referenced).
#define REGISTER_SYSTEM_TABLE_SOURCE(...) \
    namespace \
    { \
        [[maybe_unused, gnu::used, gnu::retain]] const bool REGISTER_SYSTEM_TABLE_SOURCE_CONCAT(registered_system_table_source_, __LINE__) \
            = (::DB::registerSystemTableSource(typeid(__VA_ARGS__), __builtin_FILE()), true); \
    }
