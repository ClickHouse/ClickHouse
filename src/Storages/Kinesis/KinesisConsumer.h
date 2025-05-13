#include <IO/ReadBufferFromMemory.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Core/Types.h>
#include <mutex>
#include <optional>

#include "config.h"

#if USE_AWS_KINESIS

#include <aws/kinesis/KinesisClient.h>
#include <aws/kinesis/model/Record.h>
#include <aws/kinesis/model/GetShardIteratorRequest.h>
#include <aws/kinesis/model/GetShardIteratorResult.h>
#include <aws/kinesis/model/GetRecordsRequest.h>
#include <aws/kinesis/model/GetRecordsResult.h>
#include <aws/kinesis/model/DescribeStreamRequest.h>

namespace Poco { class Logger; }

namespace DB
{

using LoggerPtr = std::shared_ptr<Poco::Logger>;

/*
    Потребитель по схеме Pull
    Читает данные из всех переданных ему шардов
    Умеет обновлять текущий список шардов (этим занимается KinesisShardBalancer)
 */
class KinesisConsumer
{
public:
    /// Структура для хранения сообщения из Kinesis
    struct Message
    {
        String data; // Полезное содержимое сообщения
        String partition_key; // Ключ разделения
        String sequence_number; // Последовательный номер записи 
        String shard_id; // Идентификатор шарда
        UInt64 approximate_arrival_timestamp = 0; // Временная метка прибытия в Kinesis
        UInt64 received_at = 0; // Время получения сообщения
    };

    enum StartingPositionType
    {
        LATEST, // С самых последних записей
        TRIM_HORIZON, // С самых старых доступных записей
        AT_TIMESTAMP // С указанного времени
    };
    
    KinesisConsumer(
        const String & stream_name_,
        const Aws::Kinesis::KinesisClient & client_,
        const std::vector<Aws::Kinesis::Model::Shard> & shards_,
        size_t max_messages_per_batch_,
        StartingPositionType starting_position_ = LATEST,
        time_t timestamp_ = 0,
        const String & consumer_name_ = "", // For debug
        size_t internal_queue_size_ = 1000);
    
    ~KinesisConsumer();

    /// Получение и обработка записей из Kinesis
    bool receive();
    
    /// Получение следующего сообщения из внутренней очереди
    std::optional<Message> getMessage();
    
    /// Инициализация и подготовка к чтению
    void init();
    
    /// Остановка потребителя
    void stop();
    
    /// Проверка статуса потребителя
    bool isRunning() const { return is_running; }

private:
    /// Получение итератора для шарда с учетом стартовой позиции
    String getShardIterator(const String & shard_id, StartingPositionType position_type, time_t timestamp_value = 0);
    
    /// Обработка полученных записей и помещение их в очередь
    void processRecords(const std::vector<Aws::Kinesis::Model::Record> & records, const String & shard_id);

    // Основные параметры
    const String stream_name;
    const Aws::Kinesis::KinesisClient & client;
    size_t max_messages_per_batch;
    StartingPositionType starting_position_type;
    time_t timestamp;
    String consumer_name; // For debug
    
    // Внутренние структуры данных
    ConcurrentBoundedQueue<Message> queue;
    std::atomic<bool> is_running{true};
    
    // Управление шардами и итераторами
    std::vector<Aws::Kinesis::Model::Shard> shards;
    std::map<String, String> shard_iterators; // shard_id -> iterator
    std::map<String, String> shard_checkpoints; // shard_id -> sequence_number
    std::mutex shard_mutex;
    
    // Статистика и диагностика
    UInt64 total_messages_received{0};
    UInt64 last_receive_time{0};
};

using KinesisConsumerPtr = std::shared_ptr<KinesisConsumer>;

}

#endif // USE_AWS_KINESIS
