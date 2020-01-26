/**
 *  Class describing a basic deliver frame
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
#include "basicframe.h"
#include "amqpcpp/stringfield.h"
#include "amqpcpp/booleanset.h"
#include "amqpcpp/connectionimpl.h"
#include "amqpcpp/deferredconsumer.h"

/**
 *  Set up namespace
 */
namespace AMQP{

/**
 *  Class implementation
 */
class BasicDeliverFrame : public BasicFrame
{
private:
    /**
     *  identifier for the consumer, valid within current channel
     *  @var ShortString
     */
    ShortString _consumerTag;

    /**
     *  server-assigned and channel specific delivery tag
     *  @var uint64_t
     */
    uint64_t _deliveryTag;

    /**
     *  indicates whether the message has been previously delivered to this (or another) client
     *  @var BooleanSet
     */
    BooleanSet _redelivered;

    /**
     *  the name of the exchange to publish to. An empty exchange name means the default exchange.
     *  @var ShortString
     */
    ShortString _exchange;

    /**
     *  Message routing key
     *  @var ShortString
     */
    ShortString _routingKey;

protected:
    /**
     * Encode a frame on a string buffer
     *
     * @param   buffer  buffer to write frame to
     */
    virtual void fill(OutBuffer& buffer) const override
    {
        BasicFrame::fill(buffer);

        _consumerTag.fill(buffer);
        buffer.add(_deliveryTag);
        _redelivered.fill(buffer);
        _exchange.fill(buffer);
        _routingKey.fill(buffer);
    }

public:
    /**
     *  Construct a basic deliver frame (client side)
     *
     *  @param  channel         channel we're working on
     *  @param  consumerTag     identifier for the consumer, valid within current channel
     *  @param  deliveryTag     server-assigned and channel specific delivery tag
     *  @param  redelivered     indicates whether the message has been previously delivered to this (or another) client
     *  @param  exchange        name of exchange to publish to
     *  @param  routingKey      message routing key
     */
    BasicDeliverFrame(uint16_t channel, const std::string& consumerTag, uint64_t deliveryTag, bool redelivered = false, const std::string& exchange = "", const std::string& routingKey = "") :
        BasicFrame(channel, (uint32_t)(consumerTag.length() + exchange.length() + routingKey.length() + 12)),
            // length of strings + 1 byte per string for stringsize, 8 bytes for uint64_t and 1 for bools
        _consumerTag(consumerTag),
        _deliveryTag(deliveryTag),
        _redelivered(redelivered),
        _exchange(exchange),
        _routingKey(routingKey)
    {}

    /**
     *  Construct a basic deliver frame from a received frame
     *
     *  @param  frame   received frame
     */
    BasicDeliverFrame(ReceivedFrame &frame) :
        BasicFrame(frame),
        _consumerTag(frame),
        _deliveryTag(frame.nextUint64()),
        _redelivered(frame),
        _exchange(frame),
        _routingKey(frame)
    {}

    /**
     *  Destructor
     */
    virtual ~BasicDeliverFrame() {}

    /**
     *  Return the name of the exchange to publish to
     *  @return  string
     */
    const std::string& exchange() const
    {
        return _exchange;
    }

    /**
     *  Return the routing key
     *  @return  string
     */
    const std::string& routingKey() const
    {
        return _routingKey;
    }

    /**
     *  Is this a synchronous frame?
     *
     *  After a synchronous frame no more frames may be
     *  sent until the accompanying -ok frame arrives
     */
    virtual bool synchronous() const override
    {
        return false;
    }

    /**
     *  Return the method ID
     *  @return  uint16_t
     */
    virtual uint16_t methodID() const override
    {
        return 60;
    }

    /**
     *  Return the server-assigned and channel specific delivery tag
     *  @return  uint64_t
     */
    uint64_t deliveryTag() const
    {
        return _deliveryTag;
    }

    /**
     *  Return the identifier for the consumer (channel specific)
     *  @return  string
     */
    const std::string& consumerTag() const
    {
        return _consumerTag;
    }

    /**
     *  Return whether the message has been previously delivered to (another) client
     *  @return  bool
     */
    bool redelivered() const
    {
        return _redelivered.get(0);
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

        // channel does not exist
        if (!channel) return false;

        // get the appropriate consumer object
        auto consumer = channel->consumer(_consumerTag);

        // skip if there was no consumer for this tag
        if (consumer == nullptr) return false;
        
        // initialize the object, because we're about to receive a message
        consumer->process(*this);

        // done
        return true;
    }
};

/**
 *  end namespace
 */
}

