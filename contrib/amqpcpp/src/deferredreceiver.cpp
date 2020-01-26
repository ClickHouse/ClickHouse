/**
 *  DeferredReceiver.cpp
 *
 *  Implementation file for the DeferredReceiver class
 *
 *  @copyright 2016 - 2018 Copernica B.V.
 */

/**
 *  Dependencies
 */
#include "amqpcpp/deferredreceiver.h"
#include "basicdeliverframe.h"
#include "basicgetokframe.h"
#include "basicheaderframe.h"
#include "bodyframe.h"

/**
 *  Start namespace
 */
namespace AMQP {

/**
 *  Initialize the object: we are going to receive a message, next frames will be header and data 
 *  @param  exchange
 *  @param  routingkey
 */
void DeferredReceiver::initialize(const std::string &exchange, const std::string &routingkey)
{
    // anybody interested in the new message?
    if (_startCallback) _startCallback(exchange, routingkey);
}

/**
 *  Process the message headers
 *
 *  @param  frame   The frame to process
 */
void DeferredReceiver::process(BasicHeaderFrame &frame)
{
    // make sure we stay in scope
    auto self = lock();

    // store the body size
    _bodySize = frame.bodySize();
    
    // is user interested in the size?
    if (_sizeCallback) _sizeCallback(_bodySize);

    // do we have a message?
    if (_message)
    {
        // store the body size and metadata
        _message->setBodySize(_bodySize);
        _message->set(frame.metaData());
    }

    // anybody interested in the headers?
    if (_headerCallback) _headerCallback(frame.metaData());

    // no body data expected? then we are now complete
    if (_bodySize == 0) complete();
}

/**
 *  Process the message data
 *
 *  @param  frame   The frame to process
 */
void DeferredReceiver::process(BodyFrame &frame)
{
    // make sure we stay in scope
    auto self = lock();

    // update the bytes still to receive
    _bodySize -= frame.payloadSize();

    // anybody interested in the data?
    if (_dataCallback) _dataCallback(frame.payload(), frame.payloadSize());

    // do we have a message? then append the data
    if (_message) _message->append(frame.payload(), frame.payloadSize());

    // if all bytes were received we are now complete
    if (_bodySize == 0) complete();
}

/**
 *  End namespace
 */
}
