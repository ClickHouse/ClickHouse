#include "KafkaBlockOutputStream.h"

namespace DB {

KafkaBlockOutputStream::KafkaBlockOutputStream(StorageKafka & storage_) : storage(storage_)
{
}

KafkaBlockOutputStream::~KafkaBlockOutputStream()
{
}

} // namespace DB
