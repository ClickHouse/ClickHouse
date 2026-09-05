#pragma once

#include <base/types.h>
#include <Interpreters/Context_fwd.h>

#include <iosfwd>

namespace DB
{

class IServerConnection;
struct ConnectionTimeouts;
class ClientInfo;

/// Writes dependency-ordered CREATE statements to `out` or one file per database in `output_dir`.
/// Empty `databases` selects every non-predefined database except `exclude_databases`.
void dumpDatabaseSchema(
    IServerConnection & connection,
    const ConnectionTimeouts & timeouts,
    const ClientInfo & client_info,
    ContextPtr context,
    const String & databases,
    const String & exclude_databases,
    const String & output_dir,
    std::ostream & out,
    std::ostream & err);

}
