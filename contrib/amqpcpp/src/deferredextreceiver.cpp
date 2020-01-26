/**
 *  DeferredExtReceiver.cpp
 *
 *  Implementation file for the DeferredExtReceiver class
 * 
 *  @author Emiel Bruijntjes <emiel.bruijntjes@copernica.com>
 *  @copyright 2018 Copernica BV
 */

/**
 *  Dependencies
 */
#include "amqpcpp/deferredextreceiver.h"
#include "amqpcpp/channelimpl.h"
 
/**
 *  Begin of namespace
 */
namespace AMQP {
    
/**
 *  Initialize the object to send out a message
 *  @param  exchange            the exchange to which the message was published
 *  @param  routingkey          the routing key that was used to publish the message
 */
void DeferredExtReceiver::initialize(const std::string &exchange, const std::string &routingkey)
{
    // call base
    DeferredReceiver::initialize(exchange, routingkey);
    
    // do we have anybody interested in messages? in that case we construct the message
    if (_messageCallback) _message.construct(exchange, routingkey);
}

/**
 *  Indicate that a message was done
 */
void DeferredExtReceiver::complete()
{
    // also monitor the channel
    Monitor monitor(_channel);

    // do we have a message?
    if (_message) _messageCallback(*_message, _deliveryTag, _redelivered);

    // do we have to inform anyone about completion?
    if (_deliveredCallback) _deliveredCallback(_deliveryTag, _redelivered);
    
    // for the next iteration we want a new message
    _message.reset();

    // do we still have a valid channel
    if (!monitor.valid()) return;

    // we are now done executing, so the channel can forget the current receiving object
    _channel->install(nullptr);
}
    
/**
 *  End of namespace
 */
}


