#include <arpa/inet.h>
#include "CARESPTRResolver.h"
#include "netdb.h"
#include "ares.h"

namespace DB {
    static void callback(void * arg, int status, int, struct hostent * host) {
        auto * ptr_records = reinterpret_cast<std::vector<std::string>*>(arg);
        if (status == ARES_SUCCESS) {
            int i = 0;
            while (auto * ptr_record = host->h_aliases[i]) {
                ptr_records->emplace_back(ptr_record);
                i++;
            }
        }
    }

    CARESPTRResolver::CARESPTRResolver() : channel(std::make_shared<ares_channel>()) {
        init();
    }

    CARESPTRResolver::~CARESPTRResolver() {
        deinit();
    }

    std::vector<std::string> CARESPTRResolver::resolve(const std::string & ip) {
        std::vector<std::string> ptr_records;

        resolve(ip, ptr_records);
        wait();

        return ptr_records;
    }

    void CARESPTRResolver::init() {
        if (ares_init(channel.get()) != ARES_SUCCESS){
            throw std::exception {};
        }
    }

    void CARESPTRResolver::deinit() {
        ares_destroy(*channel);
        ares_library_cleanup();
    }

    void CARESPTRResolver::resolve(const std::string & ip, std::vector<std::string> & response) {
        in_addr addr;
        inet_aton(ip.c_str(), &addr);

        ares_gethostbyaddr(*channel, reinterpret_cast<const char*>(&addr), sizeof(addr), AF_INET, callback, &response);
    }

    void CARESPTRResolver::wait() {
        for(;;) {
            timeval * tvp, tv;
            fd_set read_fds, write_fds;
            int nfds;

            FD_ZERO(&read_fds);
            FD_ZERO(&write_fds);
            nfds = ares_fds(*channel, &read_fds, &write_fds);
            if(nfds == 0) {
                break;
            }
            tvp = ares_timeout(*channel, nullptr, &tv);
            select(nfds, &read_fds, &write_fds, nullptr, tvp);
            ares_process(*channel, &read_fds, &write_fds);
        }
    }
}
