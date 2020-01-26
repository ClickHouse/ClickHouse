/**
 *  Base class for a message implementation
 *
 *  @copyright 2014 - 2018 Copernica BV
 */

/**
 *  Dependencies
 */
#include "basicdeliverframe.h"

/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  Class definition
 */
class ConsumedMessage : public Message
{
private:
    /**
     *  The consumer tag
     *  @var string
     */
    std::string _consumerTag;

    /**
     *  The delivery tag
     *  @var uint64_t
     */
    uint64_t _deliveryTag;

    /**
     *  Is this a redelivered message?
     *  @var bool
     */
    bool _redelivered;


public:
    /**
     *  Constructor
     *  @param  frame
     */
    ConsumedMessage(const BasicDeliverFrame &frame) :
        Message(frame.exchange(), frame.routingKey()),
        _consumerTag(frame.consumerTag()), _deliveryTag(frame.deliveryTag()), _redelivered(frame.redelivered())
    {}

    /**
     *  Constructor
     *  @param  frame
     */
    ConsumedMessage(const BasicGetOKFrame &frame) :
        Message(frame.exchange(), frame.routingKey()),
        _deliveryTag(frame.deliveryTag()), _redelivered(frame.redelivered())
    {}


    /**
     *  Destructor
     */
    virtual ~ConsumedMessage() {}

    /**
     *  Retrieve the consumer tag
     *  @return std::string
     */
    const std::string &consumer() const
    {
        return _consumerTag;
    }

    /**
     *  Report to the handler
     *  @param  callback
     */
    void report(const MessageCallback &callback)
    {
        // send ourselves to the consumer
        if (callback) callback(*this, _deliveryTag, _redelivered);
    }
};

/**
 *  End of namespace
 */
}

