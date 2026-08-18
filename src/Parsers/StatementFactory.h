#pragma once

#include <Common/Documentation.h>
#include <base/types.h>

#include <map> // STYLE_CHECK_ALLOW_STD_CONTAINERS
#include <string_view>
#include <vector> // STYLE_CHECK_ALLOW_STD_CONTAINERS

#include <boost/noncopyable.hpp>


namespace DB
{

/** A registry of all SQL statements of ClickHouse, along with their embedded documentation.
  *
  * The documentation of a statement is registered by the parser of that statement (see `REGISTER_STATEMENTS`),
  * so that it lives next to the code which implements the syntax it describes. `system.statements` exposes it.
  *
  * This is the same idea as `FunctionFactory` for functions, but much simpler: statements are not objects that
  * can be created, so the registry stores documentation only.
  */
class StatementFactory : private boost::noncopyable
{
public:
    static StatementFactory & instance();

    /// Registers the documentation of a statement. Statement names are unique; registering a name twice is an error.
    /// The `parent` field of the documentation is the name of the enclosing statement, or empty for a top-level
    /// statement. For example, `ALTER TABLE ... UPDATE` is a part of `ALTER`, and `WHERE` is a part of `SELECT`.
    void registerStatement(const String & name, Documentation documentation);

    /// The names of all registered statements, in alphabetical order.
    std::vector<String> getAllRegisteredNames() const; // STYLE_CHECK_ALLOW_STD_CONTAINERS

    /// Returns the embedded documentation of a statement (empty if none was registered).
    Documentation getDocumentation(const String & name) const;

private:
    /// An ordered map, so that `getAllRegisteredNames` returns a deterministic order.
    std::map<String, Documentation> statements; // STYLE_CHECK_ALLOW_STD_CONTAINERS
};

using StatementRegisterFunctionPtr = void (*)(StatementFactory &);

struct StatementRegisterMap : public std::map<std::string_view, StatementRegisterFunctionPtr> // STYLE_CHECK_ALLOW_STD_CONTAINERS
{
    static StatementRegisterMap & instance();
};

struct StatementRegister
{
    StatementRegister(std::string_view name, StatementRegisterFunctionPtr func_ptr)
    {
        StatementRegisterMap::instance().emplace(name, func_ptr);
    }
};

/// Calls every function defined with `REGISTER_STATEMENTS`. Must be called once at startup, before `system.statements`
/// is queried.
void registerStatements();

}

#define REGISTER_STATEMENTS_IMPL(fn, func_name, register_name) \
    void func_name(::DB::StatementFactory & factory); \
    static ::DB::StatementRegister register_name(#fn, func_name); \
    void func_name(::DB::StatementFactory & factory)

/// Defines a function which registers the documentation of the statements parsed by the current parser.
/// Place it at namespace scope in the `.cpp` file of the parser, and use the `factory` argument inside:
///
///     REGISTER_STATEMENTS(Drop)
///     {
///         factory.registerStatement("DROP", { .description = ..., .syntax = ... });
///     }
#define REGISTER_STATEMENTS(fn) REGISTER_STATEMENTS_IMPL(fn, registerStatements##fn, REGISTER_STATEMENTS_##fn)
