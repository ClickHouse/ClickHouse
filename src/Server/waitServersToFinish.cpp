#include <Server/waitServersToFinish.h>
#include <Server/ProtocolServerAdapter.h>
#include <base/sleep.h>

namespace DB
{

size_t waitServersToFinish(std::vector<DB::ProtocolServerAdapter> & servers, std::mutex & mutex, size_t seconds_to_wait)
{
    const size_t sleep_max_ms = 1000 * seconds_to_wait;
    const size_t sleep_one_ms = 100;
    size_t sleep_current_ms = 0;
    size_t current_connections = 0;
    for (;;)
    {
        current_connections = 0;

        {
            std::lock_guard lock{mutex};
            for (auto & server : servers)
            {
                server.stop();
                current_connections += server.currentConnections();
            }
        }

        if (!current_connections)
            break;

        sleep_current_ms += sleep_one_ms;
        if (sleep_current_ms < sleep_max_ms)
            sleepForMilliseconds(sleep_one_ms);
        else
            break;
    }
    return current_connections;
}

}
