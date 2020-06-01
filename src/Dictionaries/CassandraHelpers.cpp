#include <Dictionaries/CassandraHelpers.h>

#if USE_CASSANDRA
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int CASSANDRA_INTERNAL_ERROR;
}

void cassandraCheck(CassError code)
{
    if (code != CASS_OK)
        throw Exception("Cassandra driver error " + std::to_string(code) + ": " + cass_error_desc(code),
                        ErrorCodes::CASSANDRA_INTERNAL_ERROR);
}


void cassandraWaitAndCheck(CassFuturePtr && future)
{
    auto code = cass_future_error_code(future);     /// Waits if not ready
    if (code == CASS_OK)
        return;

    /// `future` owns `message` and will free it on destruction
    const char * message;
    size_t message_len;
    cass_future_error_message(future, &message, & message_len);
    std::string full_message = "Cassandra driver error " + std::to_string(code) + ": " + cass_error_desc(code) + ": " + message;
    throw Exception(full_message, ErrorCodes::CASSANDRA_INTERNAL_ERROR);
}

}

#endif
