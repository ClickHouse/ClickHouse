//          Copyright Oliver Kowalke 2015.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include <cstddef>
#include <cstdlib>
#include <map>
#include <set>
#include <iostream>
#include <string>

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/foreach.hpp>
#include <boost/make_shared.hpp>
#include <boost/ref.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/utility.hpp>

#include <boost/fiber/all.hpp>
#include "../round_robin.hpp"
#include "../yield.hpp"

using boost::asio::ip::tcp;

const std::size_t max_length = 1024;

class subscriber_session;
typedef boost::shared_ptr<subscriber_session> subscriber_session_ptr;

// a channel has n subscribers (subscriptions)
// this class holds a list of subcribers for one channel
class subscriptions {
public:
    ~subscriptions();

    // subscribe to this channel
    void subscribe( subscriber_session_ptr const& s) {
        subscribers_.insert( s);
    }

    // unsubscribe from this channel
    void unsubscribe( subscriber_session_ptr const& s) {
        subscribers_.erase(s);
    }

    // publish a message, e.g. push this message to all subscribers
    void publish( std::string const& msg);

private:
    // list of subscribers
    std::set< subscriber_session_ptr >  subscribers_;
};

// a class to register channels and to subsribe clients to this channels
class registry : private boost::noncopyable {
private:
    typedef std::map< std::string, boost::shared_ptr< subscriptions > > channels_cont;
    typedef channels_cont::iterator channels_iter;

    boost::fibers::mutex    mtx_;
    channels_cont           channels_;

    void register_channel_( std::string const& channel) {
        if ( channels_.end() != channels_.find( channel) ) {
            throw std::runtime_error("channel already exists");
        }
        channels_[channel] = boost::make_shared< subscriptions >();
        std::cout << "new channel '" << channel << "' registered" << std::endl;
    }

    void unregister_channel_( std::string const& channel) {
        channels_.erase( channel);
        std::cout << "channel '" << channel << "' unregistered" << std::endl;
    }

    void subscribe_( std::string const& channel, subscriber_session_ptr s) {
        channels_iter iter = channels_.find( channel);
        if ( channels_.end() == iter ) {
            throw std::runtime_error("channel does not exist");
        }
        iter->second->subscribe( s);
        std::cout << "new subscription to channel '" << channel << "'" << std::endl;
    }

    void unsubscribe_( std::string const& channel, subscriber_session_ptr s) {
        channels_iter iter = channels_.find( channel);
        if ( channels_.end() != iter ) {
            iter->second->unsubscribe( s);
        }
    }

    void publish_( std::string const& channel, std::string const& msg) {
        channels_iter iter = channels_.find( channel);
        if ( channels_.end() == iter ) {
            throw std::runtime_error("channel does not exist");
        }
        iter->second->publish( msg);
        std::cout << "message '" << msg << "' to publish on channel '" << channel << "'" << std::endl;
    }

public:
    // add a channel to registry
    void register_channel( std::string const& channel) {
        std::unique_lock< boost::fibers::mutex > lk( mtx_);
        register_channel_( channel);
    }

    // remove a channel from registry
    void unregister_channel( std::string const& channel) {
        std::unique_lock< boost::fibers::mutex > lk( mtx_);
        unregister_channel_( channel);
    }

    // subscribe to a channel
    void subscribe( std::string const& channel, subscriber_session_ptr s) {
        std::unique_lock< boost::fibers::mutex > lk( mtx_);
        subscribe_( channel, s);
    }

    // unsubscribe from a channel
    void unsubscribe( std::string const& channel, subscriber_session_ptr s) {
        std::unique_lock< boost::fibers::mutex > lk( mtx_);
        unsubscribe_( channel, s);
    }

    // publish a message to all subscribers registerd to the channel
    void publish( std::string const& channel, std::string const& msg) {
        std::unique_lock< boost::fibers::mutex > lk( mtx_);
        publish_( channel, msg);
    }
};

// a subscriber subscribes to a given channel in order to receive messages published on this channel
class subscriber_session : public boost::enable_shared_from_this< subscriber_session > {
public:
    explicit subscriber_session( boost::asio::io_service & io_service, registry & reg) :
        socket_( io_service),
        reg_( reg) {
    }

    tcp::socket& socket() {
        return socket_;
    }

    // this function is executed inside the fiber
    void run() {
        std::string channel;
        try {
            boost::system::error_code ec;
            // read first message == channel name
            // async_ready() returns if the the complete message is read
            // until this the fiber is suspended until the complete message
            // is read int the given buffer 'data'
            boost::asio::async_read(
                    socket_,
                    boost::asio::buffer( data_),
                    boost::fibers::asio::yield[ec]);
            if ( ec) {
                throw std::runtime_error("no channel from subscriber");
            }
            // first message ist equal to the channel name the publisher
            // publishes to
            channel = data_;
            // subscribe to new channel
            reg_.subscribe( channel, shared_from_this() );
            // read published messages
            for (;;) {
                // wait for a conditon-variable for new messages
                // the fiber will be suspended until the condtion
                // becomes true and the fiber is resumed
                // published message is stored in buffer 'data_'
                std::unique_lock< boost::fibers::mutex > lk( mtx_);
                cond_.wait( lk);
                std::string data( data_);
                lk.unlock();
                std::cout << "subscriber::run(): '" << data << std::endl;
                // message '<fini>' terminates subscription
                if ( "<fini>" == data) {
                    break;
                }
                // async. write message to socket connected with
                // subscriber
                // async_write() returns if the complete message was writen
                // the fiber is suspended in the meanwhile
                boost::asio::async_write(
                        socket_,
                        boost::asio::buffer( data, data.size() ),
                        boost::fibers::asio::yield[ec]);
                if ( ec == boost::asio::error::eof) {
                    break; //connection closed cleanly by peer
                } else if ( ec) {
                    throw boost::system::system_error( ec); //some other error
                }
                std::cout << "subscriber::run(): '" << data << "' written" << std::endl;
            }
        } catch ( std::exception const& e) {
            std::cerr << "subscriber [" << channel << "] failed: " << e.what() << std::endl;
        }
        // close socket
        socket_.close();
        // unregister channel
        reg_.unsubscribe( channel, shared_from_this() );
    }

    // called from publisher_session (running in other fiber)
    void publish( std::string const& msg) {
        std::unique_lock< boost::fibers::mutex > lk( mtx_);
        std::memset( data_, '\0', sizeof( data_));
        std::memcpy( data_, msg.c_str(), (std::min)(max_length, msg.size()));
        cond_.notify_one();
    }

private:
    tcp::socket                         socket_;
    registry                        &   reg_;
    boost::fibers::mutex                mtx_;
    boost::fibers::condition_variable   cond_;
    // fixed size message
    char                                data_[max_length];
};


subscriptions::~subscriptions() {
    BOOST_FOREACH( subscriber_session_ptr s, subscribers_) {
        s->publish("<fini>");
    } 
}

void
subscriptions::publish( std::string const& msg) {
    BOOST_FOREACH( subscriber_session_ptr s, subscribers_) {
        s->publish( msg);
    }
}

// a publisher publishes messages on its channel
// subscriber might register to this channel to get the published messages
class publisher_session : public boost::enable_shared_from_this< publisher_session > {
public:
    explicit publisher_session( boost::asio::io_service & io_service, registry & reg) :
        socket_( io_service),
        reg_( reg) {
    }

    tcp::socket& socket() {
        return socket_;
    }

    // this function is executed inside the fiber
    void run() {
        std::string channel;
        try {
            boost::system::error_code ec;
            // fixed size message
            char data[max_length];
            // read first message == channel name
            // async_ready() returns if the the complete message is read
            // until this the fiber is suspended until the complete message
            // is read int the given buffer 'data'
            boost::asio::async_read(
                    socket_,
                    boost::asio::buffer( data),
                    boost::fibers::asio::yield[ec]);
            if ( ec) {
                throw std::runtime_error("no channel from publisher");
            }
            // first message ist equal to the channel name the publisher
            // publishes to
            channel = data;
            // register the new channel
            reg_.register_channel( channel);
            // start publishing messages
            for (;;) {
                // read message from publisher asyncronous
                // async_read() suspends this fiber until the complete emssage is read
                // and stored in the given buffer 'data'
                boost::asio::async_read(
                        socket_,
                        boost::asio::buffer( data),
                        boost::fibers::asio::yield[ec]); 
                if ( ec == boost::asio::error::eof) {
                    break; //connection closed cleanly by peer
                } else if ( ec) {
                    throw boost::system::system_error( ec); //some other error
                }
                // publish message to all subscribers
                reg_.publish( channel, std::string( data) );
            }
        } catch ( std::exception const& e) {
            std::cerr << "publisher [" << channel << "] failed: " << e.what() << std::endl;
        }
        // close socket
        socket_.close();
        // unregister channel
        reg_.unregister_channel( channel);
    }

private:
    tcp::socket         socket_;
    registry        &   reg_;
};

typedef boost::shared_ptr< publisher_session > publisher_session_ptr;

// function accepts connections requests from clients acting as a publisher
void accept_publisher( boost::asio::io_service& io_service,
                       unsigned short port,
                       registry & reg) {
    // create TCP-acceptor
    tcp::acceptor acceptor( io_service, tcp::endpoint( tcp::v4(), port) );
    // loop for accepting connection requests
    for (;;) {
        boost::system::error_code ec;
        // create new publisher-session
        // this instance will be associated with one publisher
        publisher_session_ptr new_publisher_session = 
            boost::make_shared<publisher_session>( boost::ref( io_service), boost::ref( reg) );
        // async. accept of new connection request
        // this function will suspend this execution context (fiber) until a
        // connection was established, after returning from this function a new client (publisher)
        // is connected
        acceptor.async_accept(
                new_publisher_session->socket(), 
                boost::fibers::asio::yield[ec]);
        if ( ! ec) {
            // run the new publisher in its own fiber (one fiber for one client)
            boost::fibers::fiber(
                boost::bind( & publisher_session::run, new_publisher_session) ).detach();
        }
    }
}

// function accepts connections requests from clients acting as a subscriber
void accept_subscriber( boost::asio::io_service& io_service,
                        unsigned short port,
                        registry & reg) {
    // create TCP-acceptor
    tcp::acceptor acceptor( io_service, tcp::endpoint( tcp::v4(), port) );
    // loop for accepting connection requests
    for (;;) {
        boost::system::error_code ec;
        // create new subscriber-session
        // this instance will be associated with one subscriber
        subscriber_session_ptr new_subscriber_session = 
            boost::make_shared<subscriber_session>( boost::ref( io_service), boost::ref( reg) );
        // async. accept of new connection request
        // this function will suspend this execution context (fiber) until a
        // connection was established, after returning from this function a new client (subscriber)
        // is connected
        acceptor.async_accept(
                new_subscriber_session->socket(), 
                boost::fibers::asio::yield[ec]);
        if ( ! ec) {
            // run the new subscriber in its own fiber (one fiber for one client)
            boost::fibers::fiber(
                boost::bind( & subscriber_session::run, new_subscriber_session) ).detach();
        }
    }
}


int main( int argc, char* argv[]) {
    try {
        // create io_service for async. I/O
        boost::asio::io_service io_service;
        // register asio scheduler
        boost::fibers::use_scheduling_algorithm< boost::fibers::asio::round_robin >( io_service);
        // registry for channels and its subscription
        registry reg;
        // create an acceptor for publishers, run it as fiber
        boost::fibers::fiber(
            accept_publisher, boost::ref( io_service), 9997, boost::ref( reg) ).detach();
        // create an acceptor for subscribers, run it as fiber
        boost::fibers::fiber(
            accept_subscriber, boost::ref( io_service), 9998, boost::ref( reg) ).detach();
        // dispatch
        io_service.run();
        return EXIT_SUCCESS;
    } catch ( std::exception const& e) {
        std::cerr << "Exception: " << e.what() << "\n";
    }

    return EXIT_FAILURE;
}
