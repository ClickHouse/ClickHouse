#include <vector>
#include <utility>

#include <Core/Types.h>

namespace DB
{

class IOMetrics
{
public:
    
    struct Data
    {
        float tps_total;
        float tps_avg;
        float queue_size_total;
        float queue_size_avg;
        float util_total;
        float util_avg;
        float read_avg;
        float read_total;
        float write_avg;
        float write_total;
        std::vector<std::pair<String, float>> dev_read;
        std::vector<std::pair<String, float>> dev_write;
        std::vector<std::pair<String, float>> dev_queue_size;
        std::vector<std::pair<String, float>> dev_util;
        std::vector<std::pair<String, float>> dev_tps;

        Data()
        {
            tps_avg = tps_total =
            queue_size_avg = queue_size_total =  
            util_avg = util_total = 
            read_avg = read_total =
            write_avg = write_total = 0;
        }
    };

    IOMetrics();
    ~IOMetrics();

    Data get() const;

private:
};

}
