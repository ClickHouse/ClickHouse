#include <cstdint>

namespace DB
{

class OpenFDMetrics
{

public:

    struct Data
    {
        uint16_t cnt;

        Data() {
            cnt = 0;
        }
    };

    OpenFDMetrics();
    ~OpenFDMetrics();

    Data get() const;

private:
};

}
