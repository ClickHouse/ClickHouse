#include <Storages/Kinesis/KinesisConsumer.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>

#if USE_AWS_KINESIS

#include <aws/kinesis/model/RegisterStreamConsumerRequest.h>
#include <aws/core/utils/memory/AWSMemory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CONNECT_KINESIS;
    extern const int KINESIS_RESHARDING;
    extern const int TIMEOUT_EXCEEDED;
}

KinesisConsumer::KinesisConsumer(
    const String & stream_name_,
    const Aws::Kinesis::KinesisClient & client_,
    const std::vector<Aws::Kinesis::Model::Shard> & shards_,
    size_t max_messages_per_batch_,
    StartingPositionType starting_position_type_,
    time_t timestamp_,
    const String & consumer_name_,
    size_t internal_queue_size_)
    : stream_name(stream_name_)
    , client(client_)
    , max_messages_per_batch(max_messages_per_batch_)
    , starting_position_type(starting_position_type_)
    , timestamp(timestamp_)
    , consumer_name(consumer_name_)
    , queue(internal_queue_size_)
    , shards(shards_)
{
    LOG_INFO(&Poco::Logger::get("KinesisConsumer"), "KinesisConsumer created. Stream: {}, Starting position: {}",
        stream_name, 
        starting_position_type == LATEST ? "LATEST" : 
        (starting_position_type == TRIM_HORIZON ? "TRIM_HORIZON" : "AT_TIMESTAMP"));
}


void KinesisConsumer::init()
{
    // Инициализируем итераторы для каждого шарда
    for (const auto & shard : shards)
    {
        const auto & shard_id = shard.GetShardId();
        try 
        {
            std::lock_guard lock(shard_mutex);
            shard_iterators[shard_id] = getShardIterator(shard_id, starting_position_type, timestamp);
            LOG_TRACE(&Poco::Logger::get("KinesisConsumer"), "Initialized iterator for shard {}", shard_id);
        }
        catch (const std::exception & e)
        {
            LOG_WARNING(&Poco::Logger::get("KinesisConsumer"), "Failed to initialize iterator for shard {}: {}", shard_id, e.what());
            // Просто пропускаем проблемный шард
        }
    }
}

/*
    Получение итератора для шарда с учетом стартовой позиции.
*/
String KinesisConsumer::getShardIterator(const String & shard_id, StartingPositionType position_type, time_t timestamp_value)
{
    Aws::Kinesis::Model::GetShardIteratorRequest request;
    request.SetStreamName(stream_name);
    request.SetShardId(shard_id);
    
    // Преобразование нашего типа позиции в тип AWS SDK
    switch (position_type)
    {
        case LATEST:
            request.SetShardIteratorType(Aws::Kinesis::Model::ShardIteratorType::LATEST);
            break;
        case TRIM_HORIZON:
            request.SetShardIteratorType(Aws::Kinesis::Model::ShardIteratorType::TRIM_HORIZON);
            break;
        case AT_TIMESTAMP:
            request.SetShardIteratorType(Aws::Kinesis::Model::ShardIteratorType::AT_TIMESTAMP);
            
            // Преобразуем timestamp в формат AWS
            Aws::Utils::DateTime aws_timestamp(static_cast<int64_t>(timestamp_value));
            request.SetTimestamp(aws_timestamp);
            break;
    }
    
    // Если у нас есть сохраненная позиция чтения, используем ее
    if (position_type == AT_TIMESTAMP && timestamp_value == 0 && 
        shard_checkpoints.find(shard_id) != shard_checkpoints.end())
    {
        request.SetShardIteratorType(Aws::Kinesis::Model::ShardIteratorType::AFTER_SEQUENCE_NUMBER);
        request.SetStartingSequenceNumber(shard_checkpoints[shard_id]);
    }
    
    auto outcome = client.GetShardIterator(request);
    
    if (!outcome.IsSuccess())
    {
        const auto & error = outcome.GetError();
        LOG_ERROR(&Poco::Logger::get("KinesisConsumer"), 
            "Error getting shard iterator for stream {} shard {}: {} ({})", 
            stream_name, shard_id, error.GetMessage(), error.GetExceptionName());
        
        throw Exception(
            ErrorCodes::CANNOT_CONNECT_KINESIS,
            "Failed to get shard iterator for stream {} shard {}: {} ({})",
            stream_name, shard_id, error.GetMessage(), error.GetExceptionName());
    }
    
    return outcome.GetResult().GetShardIterator();
}

/*
    Эта функция получает данные из Kinesis и кладет их в очередь для дальнейшей обработки. 
    Она блокирует текущее состояние шардов, чтобы KinesisShardBalancer не выбил шард из-под ног.
*/
bool KinesisConsumer::receive()
{
    if (!is_running)
    {
        LOG_INFO(&Poco::Logger::get("KinesisConsumer"), "Consumer is stopped");
        return false;
    }
    
    LOG_TRACE(&Poco::Logger::get("KinesisConsumer"), "Starting to receive from Kinesis stream: {}", stream_name);

    // Копируем текущие итераторы под мьютексом чтобы не блокировать mutex надолго
    std::map<String, String> iterators;
    {
        std::lock_guard lock(shard_mutex);
        iterators = shard_iterators;
    }
    
    bool received_any_records = false;
    
    // Получаем данные из каждого шарда
    for (auto & [shard_id, iterator] : iterators)
    {
        if (iterator.empty())
        {
            try
            {
                std::lock_guard lock(shard_mutex);
                shard_iterators[shard_id] = getShardIterator(shard_id, starting_position_type, timestamp);
                iterator = shard_iterators[shard_id];
            }
            catch (const std::exception & e)
            {
                LOG_WARNING(&Poco::Logger::get("KinesisConsumer"), "Failed to get iterator for shard {}: {}", shard_id, e.what());
                continue;
            }
        }

        // Создаем запрос для получения записей
        Aws::Kinesis::Model::GetRecordsRequest request;
        request.SetShardIterator(iterator);
        request.SetLimit(max_messages_per_batch);
        
        try
        {
            auto outcome = client.GetRecords(request);
            
            if (!outcome.IsSuccess())
            {
                const auto & error = outcome.GetError();
                LOG_WARNING(&Poco::Logger::get("KinesisConsumer"), 
                    "Error receiving records from shard {}: {} ({})", 
                    shard_id, error.GetMessage(), error.GetExceptionName());
                
                // Если итератор устарел, нам нужен новый
                if (error.GetErrorType() == Aws::Kinesis::KinesisErrors::EXPIRED_ITERATOR 
                    || error.GetErrorType() == Aws::Kinesis::KinesisErrors::RESOURCE_NOT_FOUND)
                {
                    std::lock_guard lock(shard_mutex);
                    shard_iterators[shard_id] = ""; // Будет получен новый на следующей итерации
                }
                
                continue;
            }
            
            const auto & result = outcome.GetResult();
            const auto & records = result.GetRecords();
            
            // Сохраняем новый итератор для следующего запроса
            {
                std::lock_guard lock(shard_mutex);
                shard_iterators[shard_id] = result.GetNextShardIterator();
            }
            
            if (records.empty())
            {
                LOG_TRACE(&Poco::Logger::get("KinesisConsumer"), "No records received from shard {}", shard_id);
                continue;
            }
            
            // Обрабатываем полученные записи
            processRecords(records, shard_id);
            received_any_records = true;
            
            LOG_TRACE(&Poco::Logger::get("KinesisConsumer"), "Received {} records from shard {}", 
                records.size(), shard_id);
            
            // Если итератор пустой, значит шард закрыт
            if (result.GetNextShardIterator().empty())
            {
                LOG_INFO(&Poco::Logger::get("KinesisConsumer"), "Shard {} has been closed (empty next iterator)", shard_id);
            }
        }
        catch (const std::exception & e)
        {
            LOG_WARNING(&Poco::Logger::get("KinesisConsumer"), "Exception when receiving from shard {}: {}", shard_id, e.what());
            // Просто продолжаем работу со следующим шардом
        }
    }
    
    UInt64 now = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    last_receive_time = now;
    
    return received_any_records;
}

/*
    Преобразование записей из формата AWS SDK в наш формат и отправка в очередь.
*/
void KinesisConsumer::processRecords(const std::vector<Aws::Kinesis::Model::Record> & records, const String & shard_id)
{
    UInt64 now = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    
    for (const auto & record : records)
    {
        Message msg;
        
        // Копируем данные из AWS SDK структур в наши
        const Aws::Utils::ByteBuffer& data = record.GetData();
        msg.data = String(reinterpret_cast<const char*>(data.GetUnderlyingData()), data.GetLength());
        msg.partition_key = record.GetPartitionKey();
        msg.sequence_number = record.GetSequenceNumber();
        msg.shard_id = shard_id;
        
        // Временная метка из Kinesis (когда запись попала в поток)
        auto aws_timestamp = record.GetApproximateArrivalTimestamp();
        msg.approximate_arrival_timestamp = static_cast<UInt64>(aws_timestamp.SecondsWithMSPrecision());
        
        msg.received_at = now;
        
        // Добавляем сообщение в очередь для дальнейшей обработки
        if (!queue.tryPush(msg))
        {
            LOG_WARNING(&Poco::Logger::get("KinesisConsumer"), "Message queue is full, dropping Kinesis record");
            return;
        }
        
        // Сохраняем последний прочитанный sequence_number для восстановления позиции
        if (!msg.sequence_number.empty())
        {
            std::lock_guard lock(shard_mutex);
            shard_checkpoints[shard_id] = msg.sequence_number;
        }
        
        total_messages_received++;
    }
}

/*
    Эта функция используется KinesisSource для получения считанных данных из очереди.
*/
std::optional<KinesisConsumer::Message> KinesisConsumer::getMessage()
{
    if (!is_running)
        return std::nullopt;

    Message message;
    bool success = queue.tryPop(message);
    
    if (!success)
        return std::nullopt;
    
    return message;
}

/*
    Остановка потребителя. После вызова этой функции, потребитель больше не будет запрашивать новые сообщения, 
    ровно как отдавать уже обработанные из очереди.
*/
void KinesisConsumer::stop()
{
    LOG_INFO(&Poco::Logger::get("KinesisConsumer"), "Stopping Kinesis consumer");
    is_running = false;
    
    queue.clear();
}

/*
    Деструктор. Освобождает ресурсы.
*/
KinesisConsumer::~KinesisConsumer()
{
    stop();
    // Никаких дополнительных ресурсов AWS для освобождения нет
    // Объекты Kinesis автоматически освобождаются при уничтожении клиента
}

}

#endif // USE_AWS_KINESIS
