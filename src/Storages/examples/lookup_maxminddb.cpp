#include "config.h"

#if USE_MAXMINDDB
#    include <cerrno>
#    include <cstdio>
#    include <cstdlib>
#    include <iostream>
#    include <getopt.h>
#    include <maxminddb.h>

using namespace DB;

int main()
{
    const char * ip_address = "1.2.2.2";
    const char * db_file = "tests/queries/0_stateless/data_mmdb/GeoLite2-Country.mmdb";

    MMDB_s mmdb;
    int status = MMDB_open(db_file, MMDB_MODE_MMAP, &mmdb);
    if (status != MMDB_SUCCESS)
    {
        std::cout << "Error opening database: " << MMDB_strerror(status) << std::endl;
        return -1;
    }

    int gai_error, mmdb_error;
    MMDB_lookup_result_s result = MMDB_lookup_string(&mmdb, ip_address, &gai_error, &mmdb_error);
    if (gai_error != 0)
    {
        std::cout << "Error looking up IP address: " << gai_strerror(gai_error) << std::endl;
        return -2;
    }

    if (mmdb_error != MMDB_SUCCESS)
    {
        std::cout << "Error looking up IP address: " << MMDB_strerror(mmdb_error) << std::endl;
        return -3;
    }

    if (!result.found_entry)
    {
        std::cout << "No entry found for IP address: " << ip_address << std::endl;
        return -4;
    }

    MMDB_entry_data_s entry_data;
    int status2 = MMDB_get_value(&result.entry, &entry_data, "country", "names", "de");
    if (status2 != MMDB_SUCCESS)
    {
        std::cout << "Error getting country ISO code: " << MMDB_strerror(status2) << std::endl;
        return -5;
    }

    if (entry_data.has_data)
        std::cout << "Country Name:" << std::string(entry_data.utf8_string, entry_data.data_size) << std::endl;
    else
        std::cout << "Country Name not found" << std::endl;

    MMDB_close(&mmdb);
    return 0;
}

#endif
