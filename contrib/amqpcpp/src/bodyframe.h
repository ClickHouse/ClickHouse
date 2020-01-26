/**
 *  Class describing an AMQP Body Frame
 *
 *  @copyright 2014 - 2018 Copernica BV
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Dependencies
 */
#include "extframe.h"
#include "amqpcpp/connectionimpl.h"
#include "amqpcpp/deferredreceiver.h"

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Class implementation
 */
class BodyFrame : public ExtFrame
{
private:
    /**
     *  Payload of the frame
     *  Payload can be any number of octets
     *  @var const char *
     */
    const char *_payload;

protected:
    /**
     *  Encode a body frame to a string buffer
     *
     *  @param  buffer  buffer to write frame to
     */
    virtual void fill(OutBuffer& buffer) const override
    {
        // call base
        ExtFrame::fill(buffer);

        // add payload to buffer
        buffer.add(_payload, _size);
    }

public:
    /**
     *  Construct a body frame
     *
     *  @param  channel     channel identifier
     *  @param  payload     payload of the body
     *  @param  size        size of the payload
     */
    BodyFrame(uint16_t channel, const char *payload, uint32_t size) :
        ExtFrame(channel, size),
        _payload(payload)
    {}

    /**
     *  Constructor for incoming data
     *
     *  @param  frame   received frame to decode
     *  @return shared pointer to newly created frame
     */
    BodyFrame(ReceivedFrame& frame) :
        ExtFrame(frame),
        _payload(frame.nextData(frame.payloadSize()))
    {}

    /**
     *  Destructor
     */
    virtual ~BodyFrame() {}

    /**
     *  Return the type of frame
     *  @return     uint8_t
     */
    virtual uint8_t type() const override
    {
        return 3;
    }

    /**
     *  Return the payload of the body
     *  @return     const char *
     */
    const char *payload() const
    {
        return _payload;
    }

    /**
     *  Process the frame
     *  @param  connection      The connection over which it was received
     *  @return bool            Was it succesfully processed?
     */
    virtual bool process(ConnectionImpl *connection) override
    {
        // we need the appropriate channel
        auto channel = connection->channel(this->channel());
        
        // we must have a channel object
        if (channel == nullptr) return false;
        
        // get the object that is receiving the messages
        auto *receiver = channel->receiver();

        // check if we have a valid receiver
        if (receiver == nullptr) return false;

        // the consumer may process the frame
        receiver->process(*this);

        // done
        return true;
    }


};

/**
 *  end namespace
 */
}

