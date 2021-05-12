#include <cstdint>

namespace DB
{

class NetworkMetrics
{

public:

    struct Data
    {
	    uint64_t received_bytes;
	    uint64_t received_packets;
		uint64_t transmitted_bytes;
		uint64_t transmitted_packets;
		uint64_t tcp_retransmit;
		uint64_t tcp;
		uint64_t udp;
		uint64_t distinct_hosts;

        Data()
        {
            received_bytes = received_packets = 
            transmitted_bytes = transmitted_packets = 
            tcp_retransmit = tcp = udp = 
            distinct_hosts = 0;
        }
	};

	NetworkMetrics();
	~NetworkMetrics();

	Data get_traffic() const;
	Data get_connections() const;
	Data get() const;

private:
	int fd;
};

}

