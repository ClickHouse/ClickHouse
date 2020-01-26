/**
 *  TcpChannel.h
 *
 *  Extended channel that can be constructed on top of a TCP connection
 *
 *  @author Emiel Bruijntjes <emiel.bruijntjes@copernica.com>
 *  @copyright 2015 - 2017 Copernica BV
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Class definition
 */
class TcpChannel : public Channel
{
public:
    /**
     *  Constructor
     * 
     *  The passed in connection pointer must remain valid for the 
     *  lifetime of the channel. A constructor is thrown if the channel 
     *  cannot be connected (because the connection is already closed or
     *  because max number of channels has been reached)
     * 
     *  @param  connection
     *  @throws std::runtime_error
     */
    TcpChannel(TcpConnection *connection) :
        Channel(&connection->_connection) {}
        
    /**
     *  Destructor
     */
    virtual ~TcpChannel() {}

    /**
     *  Copying is not allowed.
     *  @param  other
     */
    TcpChannel(const TcpChannel &other) = delete;

    /**
     *  But movement is allowed
     *  @param  other
     */
    TcpChannel(TcpChannel &&other) = default;
};

/**
 *  End of namespace
 */
}

