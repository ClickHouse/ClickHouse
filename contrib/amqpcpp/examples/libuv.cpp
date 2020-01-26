/**
 *  LibUV.cpp
 * 
 *  Test program to check AMQP functionality based on LibUV
 * 
 *  @author Emiel Bruijntjes <emiel.bruijntjes@copernica.com>
 *  @copyright 2015 - 2017 Copernica BV
 */

/**
 *  Dependencies
 */
#include <uv.h>
#include <amqpcpp.h>
#include <amqpcpp/libuv.h>

/**
 *  Custom handler
 */
class MyHandler : public AMQP::LibUvHandler
{
private:
    /**
     *  Method that is called when a connection error occurs
     *  @param  connection
     *  @param  message
     */
    virtual void onError(AMQP::TcpConnection *connection, const char *message) override
    {
        std::cout << "error: " << message << std::endl;
    }

    /**
     *  Method that is called when the TCP connection ends up in a connected state
     *  @param  connection  The TCP connection
     */
    virtual void onConnected(AMQP::TcpConnection *connection) override 
    {
        std::cout << "connected" << std::endl;
    }
    
public:
    /**
     *  Constructor
     *  @param  uv_loop
     */
    MyHandler(uv_loop_t *loop) : AMQP::LibUvHandler(loop) {}

    /**
     *  Destructor
     */
    virtual ~MyHandler() = default;
};

/**
 *  Main program
 *  @return int
 */
int main()
{
    // access to the event loop
    auto *loop = uv_default_loop();
    
    // handler for libev
    MyHandler handler(loop);
    
    // make a connection
    AMQP::TcpConnection connection(&handler, AMQP::Address("amqp://guest:guest@localhost/"));
    
    // we need a channel too
    AMQP::TcpChannel channel(&connection);
    
    // create a temporary queue
    channel.declareQueue(AMQP::exclusive).onSuccess([&connection](const std::string &name, uint32_t messagecount, uint32_t consumercount) {
        
        // report the name of the temporary queue
        std::cout << "declared queue " << name << std::endl;
    });
    
    // run the loop
    uv_run(loop, UV_RUN_DEFAULT);

    // done
    return 0;
}

