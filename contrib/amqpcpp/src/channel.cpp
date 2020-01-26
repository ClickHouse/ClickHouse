/**
 *  Channel.cpp
 *
 *  Implementation file for the Channel class
 *
 *  @author Emiel Bruijntjes <emiel.bruijntjes@copernica.com>
 *  @copyright 2018 Copernica BV
 */

/**
 *  Dependencies
 */
#include "includes.h"

/**
 *  Begin of namespace
 */
namespace AMQP {

/**
 *  Construct a channel object
 * 
 *  The passed in connection pointer must remain valid for the 
 *  lifetime of the channel. Watch out: this method throws an error
 *  if the channel could not be constructed (for example because the
 *  max number of AMQP channels has been reached)
 * 
 *  @param  connection
 *  @throws std::runtime_error
 */
Channel::Channel(Connection *connection) : _implementation(new ChannelImpl())
{
    // check if the connection is indeed usable
    if (!connection->usable()) throw std::runtime_error("failed to open channel: connection is not active");
    
    // attach to the connection
    if (_implementation->attach(connection)) return;
    
    // this failed, max number of channels has been reached
    throw std::runtime_error("failed to open channel: max number of channels has been reached");
}

/**
 *  End of namespace
 */
}


 