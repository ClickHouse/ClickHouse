#pragma once

#include <Common/Documentation.h>
#include <base/types.h>

#include <map>
#include <vector>

#include <boost/noncopyable.hpp>


namespace DB
{

/// A fake "registry" of all SQL statements of ClickHouse.
/// The only purpose of this singleton is to provide embedded documentation about ClickHouse SQL statements
/// for system.statements` and `system.documentation`.
class StatementFactory : private boost::noncopyable
{
public:
    static StatementFactory & instance();

    void registerStatement(const String & name, Documentation documentation);

    std::vector<String> getAllRegisteredNames() const;
    Documentation getDocumentation(const String & name) const;

private:
    std::map<String, Documentation> statements;
};

}
