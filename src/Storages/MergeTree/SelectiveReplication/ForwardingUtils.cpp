#include <Storages/MergeTree/SelectiveReplication/ForwardingUtils.h>

#include <Client/ConnectionPool.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterserverCredentials.h>

namespace DB::SelectiveReplication::ForwardingUtils
{

ConnectionPoolPtr createReplicaPool(
    const ReplicatedMergeTreeAddress & address,
    ContextPtr context,
    unsigned pool_size)
{
    auto credentials = context->getInterserverCredentials();
    String interserver_scheme = context->getInterserverScheme();

    return ConnectionPoolFactory::instance().get(
        pool_size,
        address.host,
        address.queries_port,
        address.database,
        credentials->getUser(),
        credentials->getPassword(),
        /* proto_send_chunked */ "",
        /* proto_recv_chunked */ "",
        /* quota_key */ "",
        /* cluster */ "",
        /* cluster_secret */ "",
        "server",
        Protocol::Compression::Enable,
        (interserver_scheme == "https") ? Protocol::Secure::Enable : Protocol::Secure::Disable,
        /* bind_host */ "",
        Priority{1});
}

}
