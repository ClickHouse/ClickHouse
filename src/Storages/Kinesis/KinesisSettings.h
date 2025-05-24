#pragma once

#include <string_view>
#include <Core/Settings.h>
#include <Core/Types.h>
#include <Parsers/ASTCreateQuery.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

struct KinesisSettings : public WithContext
{
    // Required parameters
    String stream_name;                            // Имя потока Kinesis

    // AWS authorization parameters (can be taken from global configuration)
    String aws_access_key_id;                      // AWS access key ID
    String aws_secret_access_key;                  // AWS secret access key
    String aws_region = "us-east-1";               // AWS регион, где размещен поток Kinesis

    // SSL/HTTP parameters
    bool verify_ssl = false;                       // Флаг для верификации SSL сертификата
    bool use_http = false;                         // Флаг для использования HTTP вместо HTTPS (для локальной разработки)
    UInt64 request_timeout_ms = 5000;              // Таймаут запросов в миллисекундах
    UInt64 connect_timeout_ms = 5000;              // Таймаут соединения в миллисекундах
    
    // Kinesis stream parameters
    String starting_position = "LATEST";           // Начальная позиция для чтения: LATEST, TRIM_HORIZON, AT_TIMESTAMP
    UInt64 at_timestamp = 0;                       // Временная метка для позиции AT_TIMESTAMP (unix timestamp)
    bool enhanced_fan_out = false;                 // Использовать режим Enhanced Fan-Out с выделенной пропускной способностью
    String consumer_name;                          // Имя потребителя для режима Enhanced Fan-Out
    
    // Parameters for record receiving behavior
    UInt64 max_records_per_request = 10000;        // Максимальное количество записей за один запрос GetRecords
    bool auto_reconnect = true;                    // Автоматически переподключаться при сбоях
    UInt64 retry_backoff_ms = 1000;                // Интервал ожидания между повторными попытками в миллисекундах

    // Performance settings
    UInt64 max_rows_per_message = 1;               // Максимальное количество строк на одно сообщение при отправке
    UInt64 poll_timeout_ms = 500;                  // Таймаут запроса GetRecords в миллисекундах
    UInt64 num_consumers = 1;                      // Количество консумеров для чтения из шардов
    UInt64 max_block_size = 500000;                // Максимальный размер блока для обработки
    UInt64 skip_broken_messages = 0;               // Максимальное количество пропускаемых сломанных сообщений
    UInt64 flush_interval_ms = 0;                  // Таймаут для сброса данных из движка
    UInt32 max_connections = 25;                   // Максимальное количество HTTP соединений
    size_t internal_queue_size = 1000;             // Размер очереди для хранения полученных сообщений
    bool thread_per_consumer = false;              // Создавать отдельный поток для каждого потребителя
    
    // Formatting settings
    String format_name = "JSONEachRow";            // Формат сообщения (JSONEachRow, CSV, TSV и т.д.)
    String row_delimiter;                          // Разделитель строк в сообщении
    String schema;                                 // Опциональная схема для сложных форматов
    
    // Retry parameters
    UInt64 max_retries = 10;                       // Максимальное количество повторных попыток
    UInt64 retry_initial_delay_ms = 50;            // Начальная задержка между повторными попытками
    
    // TCP settings
    bool enable_tcp_keep_alive = true;             // Использовать TCP keep-alive
    UInt64 tcp_keep_alive_interval_ms = 30000;     // Интервал TCP keep-alive
    
    // Proxy settings
    String proxy_host;                             // Прокси-хост
    UInt32 proxy_port = 0;                         // Прокси-порт
    String proxy_username;                         // Имя пользователя прокси
    String proxy_password;                         // Пароль прокси

    explicit KinesisSettings(ContextPtr context_) : WithContext(context_) {}

    void loadFromQuery(const ASTStorage & storage_def);
    static bool has(std::string_view name);
};

}
