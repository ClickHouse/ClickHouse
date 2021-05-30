#include <cstdint>
#include <vector>

namespace DB
{

class SchedMetrics
{

public:
        
    struct Data
    {
        uint32_t total_csw;
            
        Data()
        {
            total_csw = 0;
        }
     };

     SchedMetrics();
     ~SchedMetrics();

     Data get() const;

private:

};

}
