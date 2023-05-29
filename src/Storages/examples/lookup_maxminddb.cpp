#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <getopt.h>
#include <maxminddb.h>
#include "config.h"

#if USE_MAXMINDDB

int main()
{
    const char * ip_address = "152.206.185.132";
    const char * db_file = "1.mmdb";

    MMDB_s mmdb;
    int status = MMDB_open(db_file, MMDB_MODE_MMAP, &mmdb);
    if (status != MMDB_SUCCESS)
    {
        fprintf(stderr, "Error opening database: %s\n", MMDB_strerror(status));
        exit(1);
    }

    int gai_error, mmdb_error;
    MMDB_lookup_result_s result = MMDB_lookup_string(&mmdb, ip_address, &gai_error, &mmdb_error);
    if (gai_error != 0)
    {
        fprintf(stderr, "Error looking up IP address: %s\n", gai_strerror(gai_error));
        exit(1);
    }

    if (mmdb_error != MMDB_SUCCESS)
    {
        fprintf(stderr, "Error looking up IP address: %s\n", MMDB_strerror(mmdb_error));
        exit(1);
    }

    if (!result.found_entry)
    {
        fprintf(stderr, "No entry found for IP address: %s\n", ip_address);
        exit(1);
    }

    MMDB_entry_data_s entry_data;
    int status2 = MMDB_get_value(&result.entry, &entry_data, "city", "names", "de");
    if (status2 != MMDB_SUCCESS)
    {
        fprintf(stderr, "Error getting country ISO code: %s\n", MMDB_strerror(status2));
        exit(1);
    }

    if (entry_data.has_data)
    {
        printf("Country ISO code: %.*s\n", entry_data.data_size, entry_data.utf8_string);
    }
    else
    {
        printf("Country ISO code not found\n");
    }

    MMDB_close(&mmdb);
    return 0;
}

#endif
