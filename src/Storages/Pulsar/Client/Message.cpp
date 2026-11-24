#include <Storages/Pulsar/Client/Message.h>

#if USE_PULSAR

#include <fmt/format.h>


namespace DB::Pulsar
{

String MessageId::toString() const
{
    if (batch_index >= 0)
        return fmt::format("{}:{}:{}", entry.ledger_id, entry.entry_id, batch_index);
    return fmt::format("{}:{}", entry.ledger_id, entry.entry_id);
}

}

#endif
