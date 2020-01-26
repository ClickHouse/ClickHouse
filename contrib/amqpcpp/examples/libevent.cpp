/**
 *  Libevent.cpp
 *
 *  Test program to check AMQP functionality based on Libevent
 *
 *  @author Brent Dimmig <brentdimmig@gmail.com>
 */

/**
 *  Dependencies
 */
#include <event2/event.h>
#include <amqpcpp.h>
#include <amqpcpp/libevent.h>


/**
 *  Main program
 *  @return int
 */
int main()
{
    // access to the event loop
    auto evbase = event_base_new();

    // handler for libevent
    AMQP::LibEventHandler handler(evbase);

    // make a connection
    AMQP::TcpConnection connection(&handler, AMQP::Address("amqp://localhost/"));

    // we need a channel too
    AMQP::TcpChannel channel(&connection);

    // create a temporary queue
    channel.declareQueue(AMQP::exclusive).onSuccess([&connection](const std::string &name, uint32_t messagecount, uint32_t consumercount) {

        // report the name of the temporary queue
        std::cout << "declared queue " << name << std::endl;

        // now we can close the connection
        connection.close();
    });

    // run the loop
    event_base_dispatch(evbase);

    event_base_free(evbase);

    // done
    return 0;
}

