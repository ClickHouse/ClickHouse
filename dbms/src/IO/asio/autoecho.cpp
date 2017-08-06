//          Copyright 2003-2013 Christopher M. Kohlhoff
//          Copyright Oliver Kowalke, Nat Goodspeed 2015.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <thread>
#include <mutex>
#include <map>

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/shared_ptr.hpp>

#include <boost/fiber/all.hpp>
#include "round_robin.hpp"
#include "yield.hpp"

using boost::asio::ip::tcp;

const int max_length = 1024;

typedef boost::shared_ptr< tcp::socket > socket_ptr;

const char* const alpha = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

/*****************************************************************************
*   thread names
*****************************************************************************/
class ThreadNames {
private:
    std::map<std::thread::id, std::string> names_{};
    const char* next_{ alpha };
    std::mutex mtx_{};

public:
    ThreadNames() = default;

    std::string lookup() {
        std::unique_lock<std::mutex> lk( mtx_);
        auto this_id( std::this_thread::get_id() );
        auto found = names_.find( this_id );
        if ( found != names_.end() ) {
            return found->second;
        }
        BOOST_ASSERT( *next_);
        std::string name(1, *next_++ );
        names_[ this_id ] = name;
        return name;
    }
};

ThreadNames thread_names;

/*****************************************************************************
*   fiber names
*****************************************************************************/
class FiberNames {
private:
    std::map<boost::fibers::fiber::id, std::string> names_{};
    unsigned next_{ 0 };
    boost::fibers::mutex mtx_{};

public:
    FiberNames() = default;

    std::string lookup() {
        std::unique_lock<boost::fibers::mutex> lk( mtx_);
        auto this_id( boost::this_fiber::get_id() );
        auto found = names_.find( this_id );
        if ( found != names_.end() ) {
            return found->second;
        }
        std::ostringstream out;
        // Bake into the fiber's name the thread name on which we first
        // lookup() its ID, to be able to spot when a fiber hops between
        // threads.
        out << thread_names.lookup() << next_++;
        std::string name( out.str() );
        names_[ this_id ] = name;
        return name;
    }
};

FiberNames fiber_names;

std::string tag() {
    std::ostringstream out;
    out << "Thread " << thread_names.lookup() << ": "
        << std::setw(4) << fiber_names.lookup() << std::setw(0);
    return out.str();
}

/*****************************************************************************
*   message printing
*****************************************************************************/
void print_( std::ostream& out) {
    out << '\n';
}

template < typename T, typename... Ts >
void print_( std::ostream& out, T const& arg, Ts const&... args) {
    out << arg;
    print_(out, args...);
}

template < typename... T >
void print( T const&... args ) {
    std::ostringstream buffer;
    print_( buffer, args...);
    std::cout << buffer.str() << std::flush;
}

/*****************************************************************************
*   fiber function per server connection
*****************************************************************************/
void session( socket_ptr sock) {
    try {
        for (;;) {
            char data[max_length];
            boost::system::error_code ec;
            std::size_t length = sock->async_read_some(
                    boost::asio::buffer( data),
                    boost::fibers::asio::yield[ec]);
            if ( ec == boost::asio::error::eof) {
                break; //connection closed cleanly by peer
            } else if ( ec) {
                throw boost::system::system_error( ec); //some other error
            }
            print( tag(), ": handled: ", std::string(data, length));
            boost::asio::async_write(
                    * sock,
                    boost::asio::buffer( data, length),
                    boost::fibers::asio::yield[ec]);
            if ( ec == boost::asio::error::eof) {
                break; //connection closed cleanly by peer
            } else if ( ec) {
                throw boost::system::system_error( ec); //some other error
            }
        }
        print( tag(), ": connection closed");
    } catch ( std::exception const& ex) {
        print( tag(), ": caught exception : ", ex.what());
    }
}

/*****************************************************************************
*   listening server
*****************************************************************************/
void server( boost::asio::io_service & io_svc, tcp::acceptor & a) {
    print( tag(), ": echo-server started");
    try {
        for (;;) {
            socket_ptr socket( new tcp::socket( io_svc) );
            boost::system::error_code ec;
            a.async_accept(
                    * socket,
                    boost::fibers::asio::yield[ec]);
            if ( ec) {
                throw boost::system::system_error( ec); //some other error
            } else {
                boost::fibers::fiber( session, socket).detach();
            }
        }
    } catch ( std::exception const& ex) {
        print( tag(), ": caught exception : ", ex.what());
    }
    io_svc.stop();
    print( tag(), ": echo-server stopped");
}

/*****************************************************************************
*   fiber function per client
*****************************************************************************/
void client( boost::asio::io_service & io_svc, tcp::acceptor & a,
             boost::fibers::barrier& barrier, unsigned iterations) {
    print( tag(), ": echo-client started");
    for (unsigned count = 0; count < iterations; ++count) {
        tcp::resolver resolver( io_svc);
        tcp::resolver::query query( tcp::v4(), "127.0.0.1", "9999");
        tcp::resolver::iterator iterator = resolver.resolve( query);
        tcp::socket s( io_svc);
        boost::asio::connect( s, iterator);
        for (unsigned msg = 0; msg < 1; ++msg) {
            std::ostringstream msgbuf;
            msgbuf << "from " << fiber_names.lookup() << " " << count << "." << msg;
            std::string message(msgbuf.str());
            print( tag(), ": Sending: ", message);
            boost::system::error_code ec;
            boost::asio::async_write(
                    s,
                    boost::asio::buffer( message),
                    boost::fibers::asio::yield[ec]);
            if ( ec == boost::asio::error::eof) {
                return; //connection closed cleanly by peer
            } else if ( ec) {
                throw boost::system::system_error( ec); //some other error
            }
            char reply[max_length];
            size_t reply_length = s.async_read_some(
                    boost::asio::buffer( reply, max_length),
                    boost::fibers::asio::yield[ec]);
            if ( ec == boost::asio::error::eof) {
                return; //connection closed cleanly by peer
            } else if ( ec) {
                throw boost::system::system_error( ec); //some other error
            }
            print( tag(), ": Reply  : ", std::string( reply, reply_length));
        }
    }
    // done with all iterations, wait for rest of client fibers
    if ( barrier.wait()) {
        // exactly one barrier.wait() call returns true
        // we're the lucky one
        a.close();
        print( tag(), ": acceptor stopped");
    }
    print( tag(), ": echo-client stopped");
}

/*****************************************************************************
*   main
*****************************************************************************/
int main( int argc, char* argv[]) {
    try {
//[asio_rr_setup
        boost::asio::io_service io_svc;
        boost::fibers::use_scheduling_algorithm< boost::fibers::asio::round_robin >( io_svc);
//]
        print( "Thread ", thread_names.lookup(), ": started");
//[asio_rr_launch_fibers
        // server
        tcp::acceptor a( io_svc, tcp::endpoint( tcp::v4(), 9999) );
        boost::fibers::fiber( server, std::ref( io_svc), std::ref( a) ).detach();
        // client
        const unsigned iterations = 2;
        const unsigned clients = 3;
        boost::fibers::barrier b( clients);
        for ( unsigned i = 0; i < clients; ++i) {
            boost::fibers::fiber(
                    client, std::ref( io_svc), std::ref( a), std::ref( b), iterations).detach();
        }
//]
//[asio_rr_run
        io_svc.run();
//]
        print( tag(), ": io_service returned");
        print( "Thread ", thread_names.lookup(), ": stopping");
        std::cout << "done." << std::endl;
        return EXIT_SUCCESS;
    } catch ( std::exception const& e) {
        print("Exception: ", e.what(), "\n");
    }
    return EXIT_FAILURE;
}
