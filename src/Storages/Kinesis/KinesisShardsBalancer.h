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

namespace DB
{

/*
    Класс для балансировки шардов между потребителями
    Он следит на состоянием стрима и если там что-то изменилось, то 
    1. Старается собрать всех консьюмеров себе (popConsumer x num_consumers)
    2. Забирает у них собранную информацию о итераторах на шарды и checkpoint'ах
    3. Распределяет шарды между потребителями
    4. Вызывает метод `updateShardList` у каждого потребителя, куда передает новый список шардов
    5. pushConsumer x num_consumers назад в StorageKinesis
*/
class KinesisShardsBalancer
{
public:

    KinesisShardsBalancer(const String & stream_name, const Aws::Kinesis::KinesisClient & client);

    /*
        Задача для фонового обновления списка шардов
        Балансер ходит в Kinesis и получает список шардов из стрима
        Если список шардов поменялся, то балансер перераспределяет шарды между потребителями
    */
    void backgroundActualizeShardListTask();

    /*
        Балансировка шардов между потребителями
    */
    void balanceShards();

private:

};

}

#endif // USE_AWS_KINESIS
